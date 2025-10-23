# Databricks notebook source
# MAGIC %md
# MAGIC # SAP_bronze_import

# COMMAND ----------


def SAP_bronze_import():
    print("[dev_first_usecase.bronze.BRONZE_TRN_SAP]からデータを取得しています……")
    df_bronze = spark.table("dev_first_usecase.bronze.BRONZE_TRN_SAP")
    df_bronze.createOrReplaceTempView("df_bronze")
    df_bronze = df_bronze.select([col(c).cast("string").alias(c) for c in df_bronze.columns])
    print(f"[dev_first_usecase.bronze.BRONZE_TRN_SAP]をデータフレームに変換しました")
    return df_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC # SAP_silver_comp

# COMMAND ----------

def SAP_silver_comp(df_bronze):	
  print("データを補足しています……")
  df_silver = spark.sql(
  """SELECT fiscal_year	AS	fiscal_year,
  period	AS	period,
  record_month	AS	record_month,
  expense_type	AS	expense_type,
  expense_type_name	AS	expense_type_name,
  account	AS	account,
  account_name	AS	account_name,
  account_detail	AS	account_detail,
  account_detail_name	AS	account_detail_name,
  plan_type	AS	plan_type,
  plan_type_name	AS	plan_type_name,
  mgmt_account_item	AS	mgmt_account_item,
  admin_cost_type	AS	admin_cost_type,
  admin_cost_type_name	AS	admin_cost_type_name,
  fin_cost_type	AS	fin_cost_type,
  fin_cost_type_name	AS	fin_cost_type_name,
  document_tp	AS	document_tp,
  document_type	AS	document_type,
  slip_number	AS	slip_number,
  line_number	AS	line_number,
  post_date	AS	post_date,
  pay_date	AS	pay_date,
  issue_section	AS	issue_section,
  issue_section_name	AS	issue_section_name,
  charge_dept	AS	charge_dept,
  charge_dept_name	AS	charge_dept_name,
  detail_text	AS	detail_text,
  sort_key	AS	sort_key,
  header_text	AS	header_text,
  allocation_dept	AS	allocation_dept,
  vendor_code	AS	vendor_code,
  vendor_name_full_width	AS	vendor_name_full_width,
  vendor_name_half_width	AS	vendor_name_half_width,
  site_id	AS	site_id,
  site_id_name	AS	site_id_name,
  division_id	AS	division_id,
  division_id_name	AS	division_id_name,
  industry_code	AS	industry_code,
  industry_code_name	AS	industry_code_name,
  org_unit	AS	org_unit,
  org_unit_name	AS	org_unit_name,
  cost_center	AS	cost_center,
  cost_center_name	AS	cost_center_name,
  business_trip_dest	AS	business_trip_dest,
  user_name	AS	user_name,
  store_name	AS	store_name,
  usage_date	AS	usage_date,
  entertainment_party	AS	entertainment_party,
  currency_type	AS	currency_type,
  actual_foreign_amount	AS	actual_foreign_amount,
  cost_center_g_level1	AS	cost_center_g_level1,
  cost_center_g_level1_name	AS	cost_center_g_level1_name,
  cost_center_g_level2	AS	cost_center_g_level2,
  cost_center_g_level2_name	AS	cost_center_g_level2_name,
  cost_center_g_level3	AS	cost_center_g_level3,
  cost_center_g_level3_name	AS	cost_center_g_level3_name,
  cost_center_g_level4	AS	cost_center_g_level4,
  cost_center_g_level4_name	AS	cost_center_g_level4_name,
  cost_center_g_level5	AS	cost_center_g_level5,
  cost_center_g_level5_name	AS	cost_center_g_level5_name,
  cost_center_g_level6	AS	cost_center_g_level6,
  cost_center_g_level6_name	AS	cost_center_g_level6_name,
  record_month	AS	record_month_min,
  record_month	AS	record_month_max,
  seq_no	AS	seq_no,
  extract_time	AS	extract_time,
  extract_date	AS	extract_date,
  reduced_budget_amount	AS	reduced_budget_amount,
  tax_amount	AS	tax_amount,
  tax_code	AS	tax_code,
  currency_code	AS	currency_code,
  current_timestamp()	AS	registration_date,
  CASE WHEN right(record_month,2) BETWEEN '04' AND '06' THEN '第１四半期' WHEN right(record_month,2) BETWEEN '07' AND '09' THEN '第２四半期' WHEN right(record_month,2) BETWEEN '10' AND '12' THEN '第３四半期' WHEN right(record_month,2) BETWEEN '01' AND '03' THEN '第４四半期' END	AS	j_fiscal_quarter,
  CASE WHEN right(record_month,2) BETWEEN '04' AND '06' THEN '1' WHEN right(record_month,2) BETWEEN '07' AND '09' THEN '2' WHEN right(record_month,2) BETWEEN '10' AND '12' THEN '3' WHEN right(record_month,2) BETWEEN '01' AND '03' THEN '4' END	AS	nj_fiscal_quarter,
  right(period,1)	AS	fiscal_month,
  left(record_month,4)	AS	year,
  right(record_month,1)	AS	month,
  fiscal_year	AS	record_year,
  CASE WHEN right(record_month,2) BETWEEN '04' AND '09' THEN '1:上期' WHEN (right(record_month,2) BETWEEN '10' AND '12')OR (right(record_month,2) BETWEEN '01' AND '03') THEN '2:下期' END	AS	fiscal_half,
  CASE WHEN right(record_month,2) BETWEEN '04' AND '09' THEN '1' WHEN (right(record_month,2) BETWEEN '10' AND '12')OR (right(record_month,2) BETWEEN '01' AND '03') THEN '2' END	AS	n_fiscal_half,
  0  AS  transportation_cost,
  0  AS  transfer_freight_storage_charge,
  0  AS  freight_storage_charge,
  0  AS  plant_site_handling_fee,
  TRY_CAST(FLOOR(TRY_CAST(REPLACE(actual_amount, ',', '') AS DECIMAL(18,2))) AS INT) AS actual_amount,
  TRY_CAST(FLOOR(TRY_CAST(REPLACE(budget_amount, ',', '') AS DECIMAL(18,2))) AS INT)	AS	budget_amount

  FROM df_bronze AS sap
  """
  )
  print("true:データの補足が完了しました")
  return df_silver


# COMMAND ----------

# MAGIC %md
# MAGIC # SAP_silver_delete

# COMMAND ----------

def SAP_silver_delete():
  if spark.catalog.tableExists('dev_first_usecase.silver.SILVER_SAP'):
    print("テーブルの既存データを削除します")
    spark.sql(f"""
      DELETE FROM dev_first_usecase.silver.silver_sap
    """)
    print(f"true:テーブルの既存データを削除しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # SAP_silver_create

# COMMAND ----------

def SAP_silver_create():
 if not spark.catalog.tableExists('dev_first_usecase.silver.SILVER_SAP'):
  print("テーブルが存在しません、新規作成を行います")
  spark.sql("""
	 CREATE TABLE dev_first_usecase.silver.silver_sap (
	  fiscal_year STRING ,
	  period STRING ,
	  record_month STRING ,
	  expense_type STRING ,
	  expense_type_name STRING ,
	  account STRING ,
	  account_name STRING ,
	  account_detail STRING ,
	  account_detail_name STRING ,
	  plan_type STRING ,
	  plan_type_name STRING ,
	  mgmt_account_item STRING ,
	  admin_cost_type STRING ,
	  admin_cost_type_name STRING ,
	  fin_cost_type STRING ,
	  fin_cost_type_name STRING ,
	  document_tp STRING ,
	  document_type STRING ,
	  slip_number STRING ,
	  line_number STRING ,
	  post_date STRING ,
	  pay_date STRING ,
	  issue_section STRING ,
	  issue_section_name STRING ,
	  charge_dept STRING ,
	  charge_dept_name STRING ,
	  detail_text STRING ,
	  sort_key STRING ,
	  header_text STRING ,
	  allocation_dept STRING ,
	  vendor_code STRING ,
	  vendor_name_full_width STRING ,
	  vendor_name_half_width STRING ,
	  site_id STRING ,
	  site_id_name STRING ,
	  division_id STRING ,
	  division_id_name STRING ,
	  industry_code STRING ,
	  industry_code_name STRING ,
	  org_unit STRING ,
	  org_unit_name STRING ,
	  cost_center STRING ,
	  cost_center_name STRING ,
	  wf_request_id STRING ,
	  business_traveler STRING ,
	  business_trip_dest STRING ,
	  business_trip_date STRING ,
	  user_name STRING ,
	  store_name STRING ,
	  usage_date STRING ,
	  entertainment_party STRING ,
	  currency_type STRING ,
	  actual_foreign_amount STRING ,
	  inout_cd STRING ,
	  product_type STRING ,
	  cost_center_g_level1 STRING ,
	  cost_center_g_level1_name STRING ,
	  cost_center_g_level2 STRING ,
	  cost_center_g_level2_name STRING ,
	  cost_center_g_level3 STRING ,
	  cost_center_g_level3_name STRING ,
	  cost_center_g_level4 STRING ,
	  cost_center_g_level4_name STRING ,
	  cost_center_g_level5 STRING ,
	  cost_center_g_level5_name STRING ,
	  cost_center_g_level6 STRING ,
	  cost_center_g_level6_name STRING ,
	  user_full_name STRING ,
	  record_month_min STRING ,
	  record_month_max STRING ,
	  gr_admin_cd STRING ,
	  gr_admin_name STRING ,
	  user_name2 STRING ,
	  seq_no STRING ,
	  extract_time STRING ,
	  extract_date STRING ,
	  reduced_budget_amount STRING ,
	  tax_amount STRING ,
	  tax_code STRING ,
	  currency_code STRING ,
	  registration_date STRING ,
	  j_fiscal_quarter STRING ,
	  nj_fiscal_quarter STRING ,
	  fiscal_month STRING ,
	  year STRING ,
	  month STRING ,
	  record_year STRING ,
	  fiscal_half STRING ,
	  n_fiscal_half STRING ,
	  transportation_cost STRING ,
	  transfer_freight_storage_charge STRING ,
	  freight_storage_charge STRING ,
	  plant_site_handling_fee STRING ,
	  actual_amount NUMERIC(15,5) ,
	  budget_amount NUMERIC(15,5)
	 )
  """)
  print("true:テーブル作成が完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # SAP_silver_incert

# COMMAND ----------

def SAP_silver_incert(df_silver):
    print("テーブルにデータをインサートします")
 
    table_cols = spark.table("dev_first_usecase.silver.silver_sap").columns
    
    # df に存在しないカラムを NULL で追加
    for col in table_cols:
      if col not in df_silver.columns:
        df_silver = df_silver.withColumn(col, lit(None))
 
    # テーブルカラム順に揃える
    df_aligned = df_silver.select(table_cols)
 
    # テーブルに書き込み
    df_aligned.write.insertInto("dev_first_usecase.silver.silver_sap", overwrite=False)
    print("true:テーブルにデータのインサートが完了しました")

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import lit
try:
    df_bronze = SAP_bronze_import()
    df_silver = SAP_silver_comp(df_bronze)
    SAP_silver_delete()
    SAP_silver_create()
    SAP_silver_incert(df_silver)
    print("Silver_SAP:TRUE")
except Exception as e:
    raise Exception(f"エラーが発生しました：{e}")
