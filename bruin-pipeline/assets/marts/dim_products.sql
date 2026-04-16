"""@bruin
name: marts.dim_products
type: bq.sql
connection: default
materialization:
  type: table
depends:
  - staging.stg_products
@bruin"""

SELECT
    product_name,
    product_series,
    sales_price
FROM `de-zoomcamp-2026-01.staging.stg_products`