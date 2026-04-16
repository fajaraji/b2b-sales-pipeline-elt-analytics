"""@bruin
name: marts.dim_accounts
type: bq.sql
connection: default
materialization:
  type: table
depends:
  - staging.stg_accounts
@bruin"""

SELECT
    account_name,
    sector,
    country,
    parent_company,
    year_established,
    revenue,
    employees
FROM `de-zoomcamp-2026-01.staging.stg_accounts`