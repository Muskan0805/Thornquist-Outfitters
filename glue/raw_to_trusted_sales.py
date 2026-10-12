"""
Glue ETL job: raw POS CSV files + raw e-commerce JSON files -> one conformed sales table
(Parquet) in the trusted bucket.

What this job does, in plain terms
----------------------------------
Reads   s3://<raw>/pos/dt=*/         (store sales, CSV, one row per sale line)
        s3://<raw>/ecommerce/dt=*/   (web and app orders, JSON lines, one order per line,
                                      each order holding a list of items)
Reshapes both into the shared "sales" schema:

    sale_id, channel, sale_date, customer_id, sku, product_name, category,
    quantity, unit_price_cad, province, store_id,
    parse_errors, source_file, ingested_at, dt

and writes it as Parquet, partitioned by dt, to   s3://<trusted>/sales_conformed/

The e-commerce conversions are:
    - one order with N items becomes N rows (sale_id = orderId-1, orderId-2, ...)
    - the ISO 8601 timestamp becomes a date (the local date of the order)
    - integer customer id 123 becomes the 6-digit text '000123'; guests stay NULL
    - SKU TH1101 becomes TH-1101
    - category 'Shoes > Hiking Boots' becomes the POS-style top category 'Footwear'
    - price in cents becomes dollars, and USD orders are converted to CAD at the
      rate in the USD_TO_CAD job argument

What it deliberately does NOT do
--------------------------------
It does not drop, de-duplicate or "fix" bad records. Anything that cannot be
converted to its proper type becomes NULL and its column name is listed in
`parse_errors`, so the data quality gate can see exactly what went wrong.
Duplicates, zero or negative quantities and prices, placeholder categories or
provinces, unknown customers and so on all pass through untouched. Catching those
is the quality gate's job. (The one exception: a line that is not valid JSON at all
is not a record, so it is skipped and counted in the log.)

The job re-reads all of raw every run and overwrites its output, so running it
twice gives the same result.
"""
import sys
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql import types as T

REQUIRED_POS_COLUMNS = [
    "txn_id", "store_id", "province", "txn_date", "sku", "product_name",
    "category", "quantity", "unit_price", "loyalty_id",
]

# E-commerce top-level category -> the POS category list. Worked out from the SKUs
# the two channels share: every SKU maps to exactly one POS category.
CATEGORY_MAP = {
    "Camp & Hike": "Camping",
    "Clothing": "Apparel",
    "Shoes": "Footwear",
    "Gear": "Accessories",
    "Bags": "Packs",
}

# 2026-09-17T09:44:40-04:00. Anything else (17/09/2026 10:15, yesterday, blank) is invalid.
ISO_TIMESTAMP = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}([+-]\d{2}:\d{2}|Z)$"

ECOM_SCHEMA = T.StructType([
    T.StructField("orderId", T.StringType()),
    T.StructField("orderTimestamp", T.StringType()),
    T.StructField("channel", T.StringType()),
    T.StructField("currency", T.StringType()),
    T.StructField("customer", T.StructType([
        T.StructField("customerId", T.LongType()),
        T.StructField("guest", T.BooleanType()),
    ])),
    T.StructField("shippingAddress", T.StructType([
        T.StructField("province", T.StringType()),
        T.StructField("country", T.StringType()),
    ])),
    T.StructField("items", T.ArrayType(T.StructType([
        T.StructField("sku", T.StringType()),
        T.StructField("title", T.StringType()),
        T.StructField("categoryPath", T.StringType()),
        T.StructField("qty", T.LongType()),
        T.StructField("priceCents", T.LongType()),
    ]))),
    # Spark puts lines that are not valid JSON here instead of failing the whole job.
    T.StructField("_corrupt_record", T.StringType()),
])


def blank_to_null(col):
    """Trim a string column and turn empty strings into NULL."""
    return F.when(F.trim(col) == "", F.lit(None)).otherwise(F.trim(col))


def normalise_sku(col):
    """POS 'TH-1001' and e-commerce 'TH1001' both become 'TH-1001'."""
    return F.regexp_replace(F.upper(blank_to_null(col)), r"^TH-?", "TH-")


def parse_errors_column(sale_date, quantity, unit_price):
    errors = F.concat_ws(
        ",",
        F.when(sale_date.isNull(), F.lit("sale_date")),
        F.when(quantity.isNull(), F.lit("quantity")),
        F.when(unit_price.isNull(), F.lit("unit_price")),
    )
    return F.when(errors == "", F.lit(None)).otherwise(errors)


# ----------------------------------------------------------------------------- POS
def conform_pos(df):
    """Turn raw POS rows (every column a string, plus the dt partition) into the sales schema."""
    missing = [c for c in REQUIRED_POS_COLUMNS + ["dt"] if c not in df.columns]
    if missing:
        raise ValueError("Raw POS data is missing expected columns: %s" % ", ".join(missing))

    # POS dates are MM/DD/YYYY. Anything that is not a real date becomes NULL.
    sale_date = F.to_date(F.trim(F.col("txn_date")), "MM/dd/yyyy")
    # N/A, blanks and other non-numbers become NULL. Negative and zero values stay.
    quantity = F.trim(F.col("quantity")).cast("int")
    unit_price = F.trim(F.col("unit_price")).cast("decimal(10,2)")

    # Customer ID: POS "L000123" -> "000123" (the common format). Blank -> NULL (walk-in customer).
    customer_id = blank_to_null(F.regexp_replace(F.upper(blank_to_null(F.col("loyalty_id"))), r"^L", ""))

    return df.select(
        blank_to_null(F.col("txn_id")).alias("sale_id"),
        F.lit("store").alias("channel"),
        sale_date.alias("sale_date"),
        customer_id.alias("customer_id"),
        normalise_sku(F.col("sku")).alias("sku"),
        blank_to_null(F.col("product_name")).alias("product_name"),
        blank_to_null(F.col("category")).alias("category"),
        quantity.alias("quantity"),
        unit_price.alias("unit_price_cad"),
        F.upper(blank_to_null(F.col("province"))).alias("province"),
        blank_to_null(F.col("store_id")).alias("store_id"),
        parse_errors_column(sale_date, quantity, unit_price).alias("parse_errors"),
        F.input_file_name().alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
        F.col("dt"),
    )


# ------------------------------------------------------------------------ E-COMMERCE
def conform_ecommerce(df, usd_to_cad):
    """Turn raw e-commerce orders (parsed with ECOM_SCHEMA, plus dt) into the sales schema.

    `usd_to_cad` is a string such as "1.35" (a string so it converts to an exact decimal).
    """
    # One row per item. posexplode_outer keeps an order whose item list is empty or
    # missing (as one row with NULL item fields) instead of silently losing it.
    items = df.select("*", F.posexplode_outer("items").alias("line_no", "item"))

    stamp = F.trim(F.col("orderTimestamp"))
    sale_date = F.when(
        stamp.rlike(ISO_TIMESTAMP), F.to_date(F.substring(stamp, 1, 10), "yyyy-MM-dd")
    )
    quantity = F.col("item.qty").cast("int")

    # Cents to dollars, then USD to CAD. An unknown currency gives NULL, never a guess.
    currency = F.upper(F.trim(F.col("currency")))
    rate = (
        F.when(currency == "CAD", F.lit(Decimal("1")))
        .when(currency == "USD", F.lit(Decimal(usd_to_cad)))
    )
    unit_price = F.round(F.col("item.priceCents").cast("decimal(18,4)") / F.lit(100) * rate, 2).cast("decimal(10,2)")

    customer_id = F.lpad(F.col("customer.customerId").cast("string"), 6, "0")

    # 'Shoes > Hiking Boots' -> 'Shoes' -> 'Footwear'. A top category that is not in the
    # map is kept as it is, so the quality gate can see it.
    top = F.trim(F.split(F.col("item.categoryPath"), ">").getItem(0))
    mapping = F.create_map(*[F.lit(x) for kv in CATEGORY_MAP.items() for x in kv])
    category = F.when(top == "", F.lit(None)).otherwise(F.coalesce(mapping[top], top))

    return items.select(
        F.concat(blank_to_null(F.col("orderId")), F.lit("-"), (F.coalesce(F.col("line_no"), F.lit(0)) + 1).cast("string")).alias("sale_id"),
        F.lower(blank_to_null(F.col("channel"))).alias("channel"),
        sale_date.alias("sale_date"),
        customer_id.alias("customer_id"),
        normalise_sku(F.col("item.sku")).alias("sku"),
        blank_to_null(F.col("item.title")).alias("product_name"),
        category.alias("category"),
        quantity.alias("quantity"),
        unit_price.alias("unit_price_cad"),
        F.upper(blank_to_null(F.col("shippingAddress.province"))).alias("province"),
        F.lit(None).cast("string").alias("store_id"),
        parse_errors_column(sale_date, quantity, unit_price).alias("parse_errors"),
        F.input_file_name().alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
        F.col("dt"),
    )


# ------------------------------------------------------------------------- READING
def configure_spark(spark):
    # Keep dt as text ("2026-09-17") instead of letting Spark guess a date type.
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    # A date that cannot be parsed gives NULL instead of failing the whole job.
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")


def read_raw_pos(spark, base):
    """Read all dated POS files under `base` (e.g. s3://<raw>/pos/), only the dt=*/ folders.

    The quarantine/ folder sits next to pos/ in the raw bucket, so it is never read.
    """
    configure_spark(spark)
    return (
        spark.read.option("header", "true")
        .option("basePath", base)
        .csv(base + "dt=*/")
    )


def read_raw_ecommerce(spark, base):
    """Read all dated e-commerce JSON-lines files under `base`. Returns (orders, bad_line_count)."""
    configure_spark(spark)
    raw = (
        spark.read.schema(ECOM_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("basePath", base)
        .json(base + "dt=*/")
        .cache()
    )
    bad = raw.filter(F.col("_corrupt_record").isNotNull()).count()
    return raw.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record"), bad


def has_files(spark, path):
    """True if the glob matches at least one file. Reading a path with no match raises an error."""
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(hconf)
    return len(fs.globStatus(p)) > 0


# ----------------------------------------------------------------------------- MAIN
def main():
    # The awsglue imports live here so the conform functions can be tested without Glue installed.
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_BUCKET", "TRUSTED_BUCKET", "USD_TO_CAD"])
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    raw = "s3://%s/" % args["RAW_BUCKET"]
    parts = []
    if has_files(spark, raw + "pos/dt=*/"):
        parts.append(conform_pos(read_raw_pos(spark, raw + "pos/")))
    else:
        print("No POS files found under %spos/ (skipping POS)" % raw)
    if has_files(spark, raw + "ecommerce/dt=*/"):
        orders, bad_lines = read_raw_ecommerce(spark, raw + "ecommerce/")
        print("E-commerce lines that were not valid JSON (skipped): %d" % bad_lines)
        print("USD to CAD rate used: %s" % args["USD_TO_CAD"])
        parts.append(conform_ecommerce(orders, args["USD_TO_CAD"]))
    else:
        print("No e-commerce files found under %secommerce/ (skipping e-commerce)" % raw)
    if not parts:
        raise RuntimeError("No input found under %s. Nothing to do." % raw)

    conformed = parts[0]
    for extra in parts[1:]:
        conformed = conformed.unionByName(extra)
    conformed = conformed.cache()

    total = conformed.count()
    if total == 0:
        raise RuntimeError("Input folders exist but contained no rows. Nothing to do.")

    out = "s3://%s/sales_conformed/" % args["TRUSTED_BUCKET"]
    conformed.write.mode("overwrite").partitionBy("dt").parquet(out)

    # These lines appear in the job's CloudWatch log: handy evidence and a sanity check.
    print("Read and wrote %d rows to %s" % (total, out))
    for row in conformed.groupBy("channel").count().orderBy("channel").collect():
        print("  channel %s: %d rows" % (row["channel"], row["count"]))
    for name in ("sale_date", "quantity", "unit_price"):
        n = conformed.filter(F.col("parse_errors").contains(name)).count()
        print("Rows where %s could not be parsed (set to NULL): %d" % (name, n))

    job.commit()


if __name__ == "__main__":
    main()
