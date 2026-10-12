"""
Glue job: raw customer extract -> conformed customers table (Parquet) in the trusted bucket.

The customer extract is a full dump of the customer list, delivered as weekly
pipe-delimited snapshots:  s3://<raw>/customers/dt=<snapshot date>/customers_<date>.txt
Each snapshot repeats nearly every customer, so this job reads only the LATEST snapshot
(the current state of the customer list) and writes it to

    s3://<trusted>/customers_conformed/

Conversions (same rule as the sales job: change the format, never judge the data):
    cust_no      '000123'       -> customer_id  '000123' (already the shared 6-digit form)
    signup_dt    '18-JAN-22'    -> signup_date  date (NULL, and listed in parse_errors, if invalid)
    province     'Ontario'      -> province     'ON' (a name not in the list is kept as it is)
    opt_in       'Y' / 'N'      -> opt_in       boolean (anything else becomes NULL)
    first/last name, email, segment_cd: trimmed; blank becomes NULL

NOT done here, on purpose: duplicate customer rows, placeholder values (N/A emails,
UNKNOWN last names, the 01-JAN-00 placeholder signup date) pass through untouched. The
data quality gate catches those.

The job is meant to run on a schedule (weekly). If no snapshot exists yet it logs that and
finishes without error. Running it twice gives the same result.
"""
import sys

from pyspark.sql import functions as F

EXPECTED_COLUMNS = [
    "cust_no", "first_name", "last_name", "email", "signup_dt", "province", "segment_cd", "opt_in",
]

PROVINCE_CODES = {
    "ALBERTA": "AB",
    "BRITISH COLUMBIA": "BC",
    "MANITOBA": "MB",
    "NEW BRUNSWICK": "NB",
    "NEWFOUNDLAND AND LABRADOR": "NL",
    "NOVA SCOTIA": "NS",
    "NORTHWEST TERRITORIES": "NT",
    "NUNAVUT": "NU",
    "ONTARIO": "ON",
    "PRINCE EDWARD ISLAND": "PE",
    "QUEBEC": "QC",
    "SASKATCHEWAN": "SK",
    "YUKON": "YT",
}


def blank_to_null(col):
    """Trim a string column and turn empty strings into NULL."""
    return F.when(F.trim(col) == "", F.lit(None)).otherwise(F.trim(col))


def conform_customers(df, snapshot_date):
    """Turn raw customer rows (all text columns) into the conformed customers schema."""
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError("Raw customer data is missing expected columns: %s" % ", ".join(missing))

    # 18-JAN-22 -> 2022-01-18. Two-digit years count from 2000. Impossible dates become NULL.
    signup_date = F.to_date(F.trim(F.col("signup_dt")), "dd-MMM-yy")

    province_text = F.upper(blank_to_null(F.col("province")))
    mapping = F.create_map(*[F.lit(x) for kv in PROVINCE_CODES.items() for x in kv])
    province = F.coalesce(mapping[province_text], province_text)

    opt_in = (
        F.when(F.upper(F.trim(F.col("opt_in"))) == "Y", F.lit(True))
        .when(F.upper(F.trim(F.col("opt_in"))) == "N", F.lit(False))
    )

    errors = F.concat_ws(
        ",",
        F.when(signup_date.isNull(), F.lit("signup_date")),
        F.when(opt_in.isNull(), F.lit("opt_in")),
    )

    return df.select(
        blank_to_null(F.col("cust_no")).alias("customer_id"),
        blank_to_null(F.col("first_name")).alias("first_name"),
        blank_to_null(F.col("last_name")).alias("last_name"),
        blank_to_null(F.col("email")).alias("email"),
        signup_date.alias("signup_date"),
        province.alias("province"),
        F.upper(blank_to_null(F.col("segment_cd"))).alias("segment_code"),
        opt_in.alias("opt_in"),
        F.when(errors == "", F.lit(None)).otherwise(errors).alias("parse_errors"),
        F.lit(snapshot_date).alias("snapshot_date"),
        F.input_file_name().alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
    )


def latest_snapshot(spark, base):
    """Return the newest dt folder name under `base` (e.g. '2026-09-28'), or None if there is none."""
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    pattern = jvm.org.apache.hadoop.fs.Path(base + "dt=*")
    fs = pattern.getFileSystem(hconf)
    names = [s.getPath().getName()[3:] for s in fs.globStatus(pattern) if s.isDirectory()]
    return max(names) if names else None


def read_snapshot(spark, base, snapshot_date):
    """Read one pipe-delimited snapshot folder. Every column stays text; no guessing."""
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    return (
        spark.read.option("header", "true")
        .option("sep", "|")
        .csv("%sdt=%s/" % (base, snapshot_date))
    )


def main():
    # The awsglue imports live here so conform_customers() can be tested without Glue installed.
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_BUCKET", "TRUSTED_BUCKET"])
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    base = "s3://%s/customers/" % args["RAW_BUCKET"]
    snapshot = latest_snapshot(spark, base)
    if snapshot is None:
        print("No customer snapshots found under %s. Nothing to do." % base)
        job.commit()
        return

    conformed = conform_customers(read_snapshot(spark, base, snapshot), snapshot).cache()
    total = conformed.count()
    out = "s3://%s/customers_conformed/" % args["TRUSTED_BUCKET"]
    conformed.write.mode("overwrite").parquet(out)

    # These lines appear in the job's CloudWatch log: handy evidence and a sanity check.
    print("Snapshot used: %s" % snapshot)
    print("Read and wrote %d rows to %s" % (total, out))
    distinct = conformed.select("customer_id").distinct().count()
    print("Distinct customer IDs: %d (the rest are repeated rows, left for the quality gate)" % (distinct,))
    for name in ("signup_date", "opt_in"):
        n = conformed.filter(F.col("parse_errors").contains(name)).count()
        print("Rows where %s could not be parsed (set to NULL): %d" % (name, n))

    job.commit()


if __name__ == "__main__":
    main()
