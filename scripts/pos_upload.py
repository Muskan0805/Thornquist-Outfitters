#!/usr/bin/env python3
"""
Upload Thornquist source files (POS CSV, e-commerce JSON lines, customer extract) into the S3 landing bucket.

What it does, in plain terms:
  1. Looks at every .csv file (.json with --format json, .txt with --format psv) in a local folder.
  2. Runs a quick sanity check on each one (not empty, has a header, has data rows).
  3. Works out the sales date from the file name (e.g. pos_2026-09-30.csv).
  4. Uploads it to  s3://<bucket>/pos/dt=<date>/<filename>
  5. Skips files that are already there with the same size, so re-running is safe.

It never edits the file contents. Landing is "data exactly as it arrived";
cleaning happens later in the pipeline.

Example:
  python pos_upload.py --bucket thornquist-landing-mk --source-dir ./pos_data
  python pos_upload.py --bucket thornquist-landing-mk --source-dir ./pos_data --dry-run
  python pos_upload.py --bucket thornquist-landing-mk --source-dir ./ecommerce --format json
  python pos_upload.py --bucket thornquist-landing-mk --source-dir ./customers --format psv
"""

import argparse
import csv
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

log = logging.getLogger("pos_upload")

# Matches 2026-09-30 or 20260930 anywhere in a file name.
DATE_PATTERNS = [
    re.compile(r"(\d{4})-(\d{2})-(\d{2})"),
    re.compile(r"(\d{4})(\d{2})(\d{2})"),
]


def extract_date(filename):
    """Return 'YYYY-MM-DD' if the file name contains a real date, else None."""
    for pattern in DATE_PATTERNS:
        for match in pattern.finditer(filename):
            year, month, day = (int(g) for g in match.groups())
            try:
                # datetime() rejects impossible dates like 2026-13-45.
                valid = datetime(year, month, day)
            except ValueError:
                continue
            if 2000 <= valid.year <= 2100:
                return valid.strftime("%Y-%m-%d")
    return None


def check_csv(path, delimiter=","):
    """Cheap arrival check. Returns an error message, or None if the file looks fine.

    This is NOT the data quality gate. It only catches files that are clearly
    broken (empty, no header, header only) before they enter the lake.
    """
    if path.stat().st_size == 0:
        return "file is empty"
    try:
        # utf-8-sig quietly handles the BOM that Excel adds to exported CSVs.
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            header = next(reader, None)
            if not header or not any(cell.strip() for cell in header):
                return "no header row"
            if delimiter != "," and len(header) < 2:
                return "header does not use the %r delimiter" % delimiter
            if next(reader, None) is None:
                return "header only, no data rows"
    except (UnicodeDecodeError, csv.Error) as exc:
        return f"cannot be read as CSV ({exc})"
    return None


def check_json_lines(path):
    """Same idea for e-commerce files: one JSON order per line. Returns an error or None."""
    if path.stat().st_size == 0:
        return "file is empty"
    try:
        with path.open("r", encoding="utf-8-sig") as f:
            first = next((line for line in f if line.strip()), "")
        if not first:
            return "no data rows"
        order = json.loads(first)
    except (UnicodeDecodeError, ValueError):
        return "first line is not valid JSON"
    if not isinstance(order, dict) or "orderId" not in order or "items" not in order:
        return "first line has no orderId or items"
    return None


def already_uploaded(s3, bucket, key, local_size):
    """True if the object exists in S3 with the same size as the local file."""
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return head["ContentLength"] == local_size


def build_key(prefix, filename):
    """Decide where in the bucket a file goes."""
    date = extract_date(filename)
    if date is None:
        # No date in the name: keep it out of the dated folders so it can't
        # pollute a partition. Someone can look at it and rename it.
        return f"{prefix}/undated/{filename}", None
    # dt=YYYY-MM-DD is Hive-style naming, which Glue crawlers and Athena
    # recognise as a partition automatically.
    return f"{prefix}/dt={date}/{filename}", date


def parse_args():
    parser = argparse.ArgumentParser(description="Upload POS CSV files to the S3 landing bucket.")
    parser.add_argument("--bucket", required=True, help="Landing bucket name, e.g. thornquist-landing-mk")
    parser.add_argument("--source-dir", required=True, help="Local folder containing the POS CSV files")
    parser.add_argument("--format", choices=["csv", "json", "psv"], default="csv",
                        help="csv = POS files (default). json = e-commerce JSON lines. psv = pipe-delimited customer extract")
    parser.add_argument("--prefix", default=None,
                        help="Top-level folder in the bucket (default: pos, ecommerce or customers)")
    parser.add_argument("--region", default=None, help="AWS region (default: from your AWS config)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without uploading")
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    ext, check, ctype, default_prefix = {
        "csv": (".csv", check_csv, "text/csv", "pos"),
        "json": (".json", check_json_lines, "application/json", "ecommerce"),
        "psv": (".txt", lambda path: check_csv(path, "|"), "text/plain", "customers"),
    }[args.format]
    prefix = args.prefix or default_prefix

    source_dir = Path(args.source_dir)
    if not source_dir.is_dir():
        log.error("Source folder not found: %s", source_dir)
        return 1

    files = sorted(p for p in source_dir.iterdir() if p.is_file() and p.suffix.lower() == ext)
    if not files:
        log.error("No %s files found in %s", ext, source_dir)
        return 1

    s3 = boto3.client("s3", region_name=args.region)

    # Fail early with a clear message if credentials or the bucket name are wrong.
    try:
        s3.head_bucket(Bucket=args.bucket)
    except (ClientError, BotoCoreError) as exc:
        log.error("Cannot reach bucket '%s': %s", args.bucket, exc)
        log.error("Check the bucket name and that your Learner Lab credentials are current "
                  "(they expire when the session ends).")
        return 1

    counts = {"uploaded": 0, "skipped": 0, "rejected": 0, "failed": 0}

    for path in files:
        problem = check(path)
        if problem:
            log.warning("REJECTED %s: %s", path.name, problem)
            counts["rejected"] += 1
            continue

        key, date = build_key(prefix, path.name)
        if date is None:
            log.warning("No date found in '%s'; it will go to %s", path.name, key)

        try:
            if already_uploaded(s3, args.bucket, key, path.stat().st_size):
                log.info("SKIPPED  %s (already in S3, same size)", path.name)
                counts["skipped"] += 1
                continue

            if args.dry_run:
                log.info("DRY RUN  would upload %s -> s3://%s/%s", path.name, args.bucket, key)
                counts["uploaded"] += 1
                continue

            s3.upload_file(
                str(path), args.bucket, key,
                ExtraArgs={"ContentType": ctype},
            )
            log.info("UPLOADED %s -> s3://%s/%s", path.name, args.bucket, key)
            counts["uploaded"] += 1
        except (ClientError, BotoCoreError) as exc:
            log.error("FAILED   %s: %s", path.name, exc)
            counts["failed"] += 1

    label = "would upload" if args.dry_run else "uploaded"
    log.info("Done. %s: %d | skipped: %d | rejected: %d | failed: %d",
             label, counts["uploaded"], counts["skipped"], counts["rejected"], counts["failed"])

    # A non-zero exit code lets a scheduler or CI job notice that something went wrong.
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
