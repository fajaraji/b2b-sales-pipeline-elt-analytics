"""@bruin
name: stg_accounts
type: bq.sql
materialization:
  type: table
  dataset: staging
depends:
  - extract_load_crm
@bruin"""

SELECT
    INITCAP(TRIM(account)) AS account_name,

    CASE 
        WHEN TRIM(LOWER(sector)) = 'technolgy' THEN 'technology'
        ELSE TRIM(LOWER(sector))
    END AS sector,

    year_established,
    
    SAFE_CAST(revenue AS FLOAT64) AS revenue,
    
    employees,
    TRIM(office_location) AS country,

    NULLIF(TRIM(subsidiary_of), 'null') AS parent_company,

    CURRENT_TIMESTAMP() AS processed_at

FROM `{{project_id}}.raw.raw_accounts`