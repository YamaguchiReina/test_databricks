{{ config(
  materialized='table',
  catalog='dev_first_usecase',
  schema='silver',
  alias='silver_sbt_sap'
) }}



WITH bronze_trn_sap AS (
  SELECT * FROM {{ source('dev_first_usecase', 'bronze_trn_sap') }}
),
silver_test_sql AS (
  SELECT
  record_month,
  plan_type,
  plan_type_name,
  fiscal_year,
  period,
  cost_center,
  cost_center_name,
  budget_amount,
  actual_amount
  FROM bronze_trn_sap
)



SELECT * FROM silver_test_sql;
 