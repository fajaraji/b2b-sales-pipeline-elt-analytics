"""@bruin
name: staging.stg_sales_teams
type: bq.sql
connection: default
materialization:
  type: table
depends:
  - extract_load_crm
@bruin"""

SELECT
    TRIM(sales_agent) AS sales_agent,
    TRIM(manager) AS manager,
    TRIM(regional_office) AS regional_office
FROM `de-zoomcamp-2026-01.raw.raw_sales_teams`