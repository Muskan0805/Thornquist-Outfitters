# Thornquist Outfitters pipeline runbook

How to rebuild the full pipeline in a fresh AWS Academy Learner Lab account, and what to check at each step. Replace `<SUFFIX>` with your short suffix (we used `david26`) and `<ACCOUNT_ID>` with your account number. Run everything from the repo root.

## Before you start

1. Start the Learner Lab and paste fresh credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, region us-east-1).
2. Confirm access: `aws sts get-caller-identity`.
3. Update the repo: `git checkout david/quality-rules && git pull`.

## Step 1: Buckets stack

```
aws cloudformation deploy \
  --template-file infra/thornquist-buckets.yaml \
  --stack-name thornquist-buckets \
  --parameter-overrides LabRoleArn=arn:aws:iam::<ACCOUNT_ID>:role/LabRole Suffix=<SUFFIX> EnableZoneLockdown=false
```

Creates the landing, raw, trusted, and curated buckets. Leave lockdown off until everything else works (see Step 9).

## Step 2: Glue and Athena stack

```
aws cloudformation deploy \
  --template-file infra/thornquist-glue-athena.yaml \
  --stack-name thornquist-glue-athena \
  --parameter-overrides LabRoleArn=arn:aws:iam::<ACCOUNT_ID>:role/LabRole Suffix=<SUFFIX>
```

Creates the `thornquist_raw` catalog database, the raw crawler, the Athena workgroup `thornquist-<SUFFIX>`, and the Athena results bucket.

## Step 3: ETL stack

```
aws cloudformation deploy \
  --template-file infra/thornquist-etl.yaml \
  --stack-name thornquist-etl \
  --parameter-overrides LabRoleArn=arn:aws:iam::<ACCOUNT_ID>:role/LabRole Suffix=<SUFFIX> TrustedDatabaseName=thornquist_trusted_<SUFFIX>
```

Use a unique `TrustedDatabaseName`. The default `thornquist_trusted` may already exist in the account and fails with "Database already exists."

## Step 4: Load the POS data

Upload the Glue script to the scripts bucket, then load the POS files into landing:

```
aws s3 cp glue/raw_to_trusted_pos.py s3://thornquist-scripts-<SUFFIX>/glue/raw_to_trusted_pos.py
python3 scripts/generate_mock_data.py --out mock_data
python3 scripts/pos_upload.py --bucket thornquist-landing-<SUFFIX> --source-dir mock_data/pos --dry-run
python3 scripts/pos_upload.py --bucket thornquist-landing-<SUFFIX> --source-dir mock_data/pos
```

Expect 14 uploads. `pos_2026-10-01.csv` (empty) and `pos_2026-10-02.csv` (header only) are rejected on purpose.

If `pos_upload.py` segfaults on this machine's Python 3.7, copy the files with the AWS CLI instead:

```
for f in mock_data/pos/pos_*.csv; do
  name=$(basename "$f")
  date=$(echo "$name" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}')
  if [ -s "$f" ] && [ "$(wc -l < "$f")" -gt 1 ]; then
    aws s3 cp "$f" "s3://thornquist-landing-<SUFFIX>/pos/dt=$date/$name" --content-type text/csv
  fi
done
```

The landing-to-raw Lambda copies the files to raw. Check that raw has 14 POS files.

## Step 5: Crawl raw, run the Glue job, crawl trusted

```
aws glue start-crawler --name thornquist-raw-pos-<SUFFIX>
aws glue get-crawler --name thornquist-raw-pos-<SUFFIX> --query "Crawler.State" --output text   # wait for READY
aws glue start-job-run --job-name thornquist-raw-to-trusted-pos-<SUFFIX>
aws glue get-job-run --job-name thornquist-raw-to-trusted-pos-<SUFFIX> --run-id <JobRunId> --query "JobRun.JobRunState" --output text   # wait for SUCCEEDED
aws glue start-crawler --name thornquist-trusted-<SUFFIX>
```

Expected: 2,734 POS rows in the trusted table `sales_conformed`, across 14 `dt=` partitions.

## Step 6: E-commerce (direct to raw)

E-commerce skips landing and the Lambda to keep the pipeline simple. Upload the JSON, then describe it to Athena:

```
aws s3 cp mock_data/ecommerce/ s3://thornquist-raw-<SUFFIX>/ecommerce/ --recursive
```

Run statements 1 and 2 in `sql/pipeline.sql` in the Athena console (workgroup `thornquist-<SUFFIX>`). Expect 1,899 item rows in `sales_ecommerce`.

## Step 7: Combine and apply the quality gate

Run statements 3 to 6 in `sql/pipeline.sql`:

- `sales_all`: POS and e-commerce combined, 4,633 rows
- `quarantine_all`: rows that fail any rule, 252 rows
- `curated_all`: rows that pass every rule, 4,381 rows

Statement 6 checks that curated plus quarantine equals all rows.

## Step 8: Export to the curated zone

Clear the curated bucket first, then export with Athena:

```sql
UNLOAD (SELECT * FROM thornquist_trusted_<SUFFIX>.curated_all)
TO 's3://thornquist-curated-<SUFFIX>/sales_clean/' WITH (format = 'PARQUET');

UNLOAD (SELECT * FROM thornquist_trusted_<SUFFIX>.quarantine_all)
TO 's3://thornquist-curated-<SUFFIX>/quarantine/' WITH (format = 'PARQUET');
```

## Step 9: Bucket lockdown

Once everything else works, turn lockdown on. Set `AdminPrincipalArn` to your own sign-in role, so you keep access:

```
aws cloudformation deploy \
  --template-file infra/thornquist-buckets.yaml \
  --stack-name thornquist-buckets \
  --parameter-overrides LabRoleArn=arn:aws:iam::<ACCOUNT_ID>:role/LabRole Suffix=<SUFFIX> EnableZoneLockdown=true AdminPrincipalArn=arn:aws:iam::<ACCOUNT_ID>:role/voclabs
```

Check the landing bucket policy denies everyone except LabRole and the admin role.

## Known issues and fixes

| Problem | Fix |
|---|---|
| Database already exists: thornquist_trusted | Set TrustedDatabaseName=thornquist_trusted_<SUFFIX> |
| Athena: external_location not allowed by the workgroup | Drop external_location from CREATE TABLE AS. Use UNLOAD to write to a specific S3 location |
| INVALID_CAST_ARGUMENT: 2026-09-31 | Wrap the date cast in TRY(...) so impossible dates become NULL |
| Packs category missing from e-commerce rows | Map categoryPath starting with Bags to Packs |
| Stack delete fails: bucket is not empty | Empty versioned buckets, including old versions, before deleting the stack |
| Glue job script not found | Upload glue/raw_to_trusted_pos.py to the scripts bucket before starting the job |

## Decisions and assumptions

- Currency: USD orders are converted at 1.4250 CAD per USD, the average of Bank of Canada daily rates for 5 to 9 October 2026.
- Category mapping: Clothing to Apparel, Shoes to Footwear, Camp & Hike to Camping, Gear to Accessories, Bags to Packs.
- E-commerce path: files go straight to raw, with no landing zone or Lambda. The report should say so.
- Quality rules: five rules, documented in notes/quality-rules.md. Rule 4 quarantines every copy of a duplicate sale ID (34 IDs, 69 rows, of which 35 are extra copies).
- Data quality finding: 13 e-commerce items have a price of $0.00 or -$0.01, and rule 3 catches them. Impossible dates (2026-09-31) are caught by rule 1.

## Expected results

| Check | Value |
|---|---|
| POS rows in trusted | 2,734 |
| E-commerce item rows | 1,899 |
| Combined rows | 4,633 |
| Curated (passed) | 4,381 |
| Quarantined | 252 |
| Revenue, curated | about $696,987 CAD |
