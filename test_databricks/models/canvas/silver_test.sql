WITH bronze_trn_sap AS (
  SELECT
    *
  FROM {{ source('dev_first_usecase', 'bronze_trn_sap') }}
), silver_test_sql AS (
  SELECT
    *
  FROM bronze_trn_sap
)
SELECT
  *
FROM silver_test_sql