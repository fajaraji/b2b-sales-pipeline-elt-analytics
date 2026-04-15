"""@bruin
name: staging.stg_products
type: bq.sql
connection: default
materialization:
  type: table
depends:
  - extract_load_crm
@bruin"""

SELECT
    TRIM(product) AS product_name,
    TRIM(series) AS product_series,
    SAFE_CAST(sales_price AS INT64) AS sales_price
FROM `de-zoomcamp-2026-01.raw.raw_products`