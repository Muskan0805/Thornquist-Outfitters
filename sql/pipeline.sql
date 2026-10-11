-- Thornquist Outfitters: Athena SQL for the POS and e-commerce pipeline.
-- Workgroup: thornquist-david26. Run the statements in order.

-- 1. Describe the e-commerce JSON files (uploaded to s3://thornquist-raw-david26/ecommerce/)
CREATE EXTERNAL TABLE thornquist_raw.ecommerce_orders (
  orderId string,
  orderTimestamp string,
  channel string,
  currency string,
  customer struct<customerId:bigint, guest:boolean>,
  shippingAddress struct<province:string, country:string>,
  items array<struct<sku:string, title:string, categoryPath:string, qty:int, priceCents:int>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://thornquist-raw-david26/ecommerce/';

-- 2. Flatten e-commerce orders to one row per item, map to the shared schema, convert USD at 1.4250
CREATE TABLE thornquist_trusted_david26.sales_ecommerce
WITH (format = 'PARQUET') AS
SELECT
  CONCAT(o.orderId, '-', CAST(t.n AS varchar)) AS sale_id,
  'ecommerce' AS channel,
  TRY(CAST(substr(o.orderTimestamp, 1, 10) AS date)) AS sale_date,
  CAST(o.customer.customerId AS varchar) AS customer_id,
  REPLACE(t.item.sku, 'TH', 'TH-') AS sku,
  t.item.title AS product_name,
  CASE
    WHEN t.item.categoryPath LIKE 'Clothing%' THEN 'Apparel'
    WHEN t.item.categoryPath LIKE 'Shoes%' THEN 'Footwear'
    WHEN t.item.categoryPath LIKE 'Camp & Hike%' THEN 'Camping'
    WHEN t.item.categoryPath LIKE 'Gear%' THEN 'Accessories'
    WHEN t.item.categoryPath LIKE 'Bags%' THEN 'Packs'
    ELSE 'UNKNOWN'
  END AS category,
  t.item.qty AS quantity,
  CAST(t.item.priceCents / 100.0 * IF(o.currency = 'USD', 1.4250, 1.0) AS decimal(10,2)) AS unit_price_cad,
  o.shippingAddress.province AS province
FROM thornquist_raw.ecommerce_orders o
CROSS JOIN UNNEST(o.items) WITH ORDINALITY AS t(item, n);

-- 3. Combine POS (sales_conformed) and e-commerce into one table
CREATE TABLE thornquist_trusted_david26.sales_all
WITH (format = 'PARQUET') AS
SELECT sale_id, channel, sale_date, customer_id, sku, product_name, category,
       quantity, unit_price_cad, province, store_id
FROM thornquist_trusted_david26.sales_conformed
UNION ALL
SELECT sale_id, channel, sale_date, customer_id, sku, product_name, category,
       quantity, unit_price_cad, province, CAST(NULL AS varchar) AS store_id
FROM thornquist_trusted_david26.sales_ecommerce;

-- 4. Quarantine: every row that fails at least one of the five quality rules, with the reasons
CREATE TABLE thornquist_trusted_david26.quarantine_all
WITH (format = 'PARQUET') AS
SELECT s.*,
  CONCAT_WS(',',
    IF(s.sale_date IS NULL, 'R1_sale_date'),
    IF(s.quantity IS NULL OR s.quantity <= 0, 'R2_quantity'),
    IF(s.unit_price_cad IS NULL OR s.unit_price_cad <= 0, 'R3_unit_price'),
    IF(d.sale_id IS NOT NULL, 'R4_duplicate_sale_id'),
    IF(s.category IS NULL OR TRIM(s.category) = '' OR UPPER(s.category) = 'UNKNOWN', 'R5_category')) AS fail_reasons
FROM thornquist_trusted_david26.sales_all s
LEFT JOIN (
  SELECT sale_id FROM thornquist_trusted_david26.sales_all
  GROUP BY sale_id HAVING COUNT(*) > 1
) d ON s.sale_id = d.sale_id
WHERE s.sale_date IS NULL
   OR s.quantity IS NULL OR s.quantity <= 0
   OR s.unit_price_cad IS NULL OR s.unit_price_cad <= 0
   OR d.sale_id IS NOT NULL
   OR s.category IS NULL OR TRIM(s.category) = '' OR UPPER(s.category) = 'UNKNOWN';

-- 5. Curated: only rows that pass all five rules
CREATE TABLE thornquist_trusted_david26.curated_all
WITH (format = 'PARQUET') AS
SELECT s.*
FROM thornquist_trusted_david26.sales_all s
LEFT JOIN (
  SELECT sale_id FROM thornquist_trusted_david26.sales_all
  GROUP BY sale_id HAVING COUNT(*) > 1
) d ON s.sale_id = d.sale_id
WHERE s.sale_date IS NOT NULL
  AND s.quantity > 0
  AND s.unit_price_cad > 0
  AND d.sale_id IS NULL
  AND s.category IS NOT NULL AND TRIM(s.category) <> ''
  AND UPPER(s.category) <> 'UNKNOWN';

-- 6. Reconciliation: curated + quarantine should equal all rows
SELECT
  (SELECT COUNT(*) FROM thornquist_trusted_david26.sales_all) AS all_rows,
  (SELECT COUNT(*) FROM thornquist_trusted_david26.curated_all) AS curated_rows,
  (SELECT COUNT(*) FROM thornquist_trusted_david26.quarantine_all) AS quarantine_rows;
