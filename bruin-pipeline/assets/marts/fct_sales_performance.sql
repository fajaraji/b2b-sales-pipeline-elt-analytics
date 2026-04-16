"""@bruin
name: marts.fct_sales_performance
type: bq.sql
connection: default
materialization:
  type: table
  partition_by: close_date
  cluster_by:
    - sales_agent
    - deal_stage
depends:
  - staging.stg_sales_pipeline
  - marts.dim_accounts
  - marts.dim_products
  - marts.dim_sales_teams
@bruin"""

SELECT

    p.opportunity_id,
    p.sales_agent,
    p.product_name,
    p.account_name,
    p.deal_stage,
    p.engage_date,
    p.close_date,
    
    a.sector,
    a.country,
    t.manager,
    t.regional_office,
    pr.product_series,
    
    p.close_value,
    CASE WHEN p.deal_stage = 'Won' THEN 1 ELSE 0 END AS is_won,
    CASE WHEN p.deal_stage = 'Won' THEN p.close_value ELSE 0 END AS revenue_won,
    DATE_DIFF(p.close_date, p.engage_date, DAY) AS days_to_close

FROM `de-zoomcamp-2026-01.staging.stg_sales_pipeline` p
LEFT JOIN `de-zoomcamp-2026-01.marts.dim_accounts` a 
    ON p.account_name = a.account_name
LEFT JOIN `de-zoomcamp-2026-01.marts.dim_sales_teams` t 
    ON p.sales_agent = t.sales_agent
LEFT JOIN `de-zoomcamp-2026-01.marts.dim_products` pr 
    ON p.product_name = pr.product_name