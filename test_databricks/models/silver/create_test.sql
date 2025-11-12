{{ config(
  materialized='table',
  catalog='dev_first_usecase',
  schema='silver',
  alias='silver_dbt_sap'
) }}

WITH bronze_mst_account AS (
  SELECT
    ID,
    S4_SAPCode1
  FROM {{ source('dev_first_usecase', 'bronze_mst_account') }}
), filter AS (
  SELECT
    *
  FROM bronze_mst_account
  WHERE
    NOT S4_SAPCode1 IS NULL
), untitled_sql AS (
  SELECT
    *
  FROM filter
)
SELECT
  *
FROM untitled_sql