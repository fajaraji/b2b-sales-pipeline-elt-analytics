"""@bruin
name: staging.stg_sales_pipeline
type: bq.sql
connection: default
materialization:
  type: table
depends:
  - extract_load_crm
@bruin"""

SELECT
    TRIM(opportunity_id) AS opportunity_id,
    TRIM(sales_agent) AS sales_agent,
    TRIM(product) AS product_name,
    
    -- Mengubah string 'null' menjadi NULL asli agar bisa di-join dengan aman
    NULLIF(TRIM(account), 'null') AS account_name,
    
    TRIM(deal_stage) AS deal_stage,
    
    -- Casting string menjadi format tanggal
    SAFE_CAST(engage_date AS DATE) AS engage_date,
    SAFE_CAST(NULLIF(TRIM(close_date), 'null') AS DATE) AS close_date,
    
    -- Memastikan value berupa angka desimal
    SAFE_CAST(close_value AS FLOAT64) AS close_value

FROM `de-zoomcamp-2026-01.raw.raw_sales_pipeline`