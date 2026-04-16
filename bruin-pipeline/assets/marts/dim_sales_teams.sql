"""@bruin
name: marts.dim_sales_teams
type: bq.sql
connection: default
materialization:
  type: table
depends:
  - staging.stg_sales_teams
@bruin"""

SELECT
    sales_agent,
    manager,
    regional_office
FROM `de-zoomcamp-2026-01.staging.stg_sales_teams`