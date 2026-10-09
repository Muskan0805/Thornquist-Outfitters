"""
Glue ETL job: raw POS CSV files -> conformed sales table (Parquet) in the trusted bucket.

What this job does, in plain terms
----------------------------------
Reads every daily POS file under   s3://<raw>/pos/dt=*/
Reshapes it into the shared "sales" schema that all three channels will use:

    sale_id, channel, sale_date, customer_id, sku, product_name, category,
    quantity, unit_price_cad, province, store_id,
    parse_errors, source_file, ingested_at, dt

and writes it as Parquet, partitioned by dt, to   s3://<trusted>/sales_conformed/

What it deliberately does NOT do
--------------------------------
It does not drop, de-duplicate or "fix" bad records. Anything that cannot be
converted to its proper type becomes NULL and its column name is listed in
`parse_errors`, so the data quality gate can see exactly what went wrong.
Duplicates, zero or negative quantities, placeholder categories, unknown
customers and so on all pass through untouched. Catching those is the quality
gate's job (uniqueness, range, completeness and referential-integrity rules).

The job re-reads all of raw every run and overwrites its output, so running it
twice gives the same result.
"""
import sys

from pyspark.sql import functions as F

CHANNEL = "store"
REQUIRED_COLUMNS = [
    "txn_id", "store_id", "province", "txn_date", "sku", "product_name",
    "category", "quantity", "unit_price", "loyalty_id",
]


def blank_to_null(col):
    """Trim a string column and turn empty strings into NULL."""
    return F.when(F.trim(col) == "", F.lit(None)).otherwise(F.trim(col))


def conform_pos(df):
    """Turn raw POS rows (every column a string, plus the dt partition) into the sales schema."""
    missing = [c for c in REQUIRED_COLUMNS + ["dt"] if c not in df.columns]
    if missing:
        raise ValueError("Raw POS data is missing expected columns: %s" % ", ".join(missing))

    # POS dates are MM/DD/YYYY. Anything that is not a real date becomes NULL.
    sale_date = F.to_date(F.trim(F.col("txn_date")), "MM/dd/yyyy")
    # N/A, blanks and other non-numbers become NULL. Negative and zero values stay.
    quantity = F.trim(F.col("quantity")).cast("int")
    unit_price = F.trim(F.col("unit_price")).cast("decimal(10,2)")

    # Customer ID: POS "L000123" -> "000123" (the common format). Blank -> NULL (walk-in customer).
    customer_id = blank_to_null(F.regexp_replace(F.upper(blank_to_null(F.col("loyalty_id"))), r"^L", ""))
    # SKU: POS "TH-1001" and e-commerce "TH1001" both become "TH-1001".
    sku = F.regexp_replace(F.upper(blank_to_null(F.col("sku"))), r"^TH-?", "TH-")

    errors = F.concat_ws(
        ",",
        F.when(sale_date.isNull(), F.lit("sale_date")),
        F.when(quantity.isNull(), F.lit("quantity")),
        F.when(unit_price.isNull(), F.lit("unit_price")),
    )

    return df.select(
        blank_to_null(F.col("txn_id")).alias("sale_id"),
        F.lit(CHANNEL).alias("channel"),
        sale_date.alias("sale_date"),
        customer_id.alias("customer_id"),
        sku.alias("sku"),
        blank_to_null(F.col("product_name")).alias("product_name"),
        blank_to_null(F.col("category")).alias("category"),
        quantity.alias("quantity"),
        unit_price.alias("unit_price_cad"),
        F.upper(blank_to_null(F.col("province"))).alias("province"),
        blank_to_null(F.col("store_id")).alias("store_id"),
        F.when(errors == "", F.lit(None)).otherwise(errors).alias("parse_errors"),
        F.input_file_name().alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
        F.col("dt"),
    )


def read_raw_pos(spark, base):
    """Read all dated POS files under `base` (e.g. s3://<raw>/pos/), only the dt=*/ folders.

    The quarantine/ folder sits next to pos/ in the raw bucket, so it is never read.
    """
    # Keep dt as text ("2026-09-17") instead of letting Spark guess a date type.
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    # A date that cannot be parsed gives NULL instead of failing the whole job.
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    return (
        spark.read.option("header", "true")
        .option("basePath", base)
        .csv(base + "dt=*/")
    )


def main():
    # The awsglue imports live here so conform_pos() can be tested without Glue installed.
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_BUCKET", "TRUSTED_BUCKET"])
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    raw = read_raw_pos(spark, "s3://%s/pos/" % args["RAW_BUCKET"])
    conformed = conform_pos(raw).cache()

    total = conformed.count()
    if total == 0:
        raise RuntimeError("No POS rows found under s3://%s/pos/. Nothing to do." % args["RAW_BUCKET"])

    out = "s3://%s/sales_conformed/" % args["TRUSTED_BUCKET"]
    conformed.write.mode("overwrite").partitionBy("dt").parquet(out)

    # These lines appear in the job's CloudWatch log: handy evidence and a sanity check.
    print("Read and wrote %d rows to %s" % (total, out))
    for name in ("sale_date", "quantity", "unit_price"):
        n = conformed.filter(F.col("parse_errors").contains(name)).count()
        print("Rows where %s could not be parsed (set to NULL): %d" % (name, n))

    job.commit()


if __name__ == "__main__":
    main()
