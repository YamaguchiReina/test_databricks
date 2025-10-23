# Databricks notebook source
# MAGIC %md
# MAGIC # SAP_Silver_import

# COMMAND ----------

def SAP_Silver_import():
    print("[dev_first_usecase.silver.silver_sap]からデータを取得しています……")
    df_sap = spark.table("dev_first_usecase.silver.silver_sap")
    print(f"[dev_first_usecase.silver.silver_sap]をデータフレームに変換しました")
    return df_sap

# COMMAND ----------

# MAGIC %md
# MAGIC # Sactona_Silver_import

# COMMAND ----------

def Sactona_Silver_import():
    print("[dev_first_usecase.silver.silver_sactona]からデータを取得しています……")
    df_sactona = spark.table("dev_first_usecase.silver.silver_sactona")
    print(f"[dev_first_usecase.silver.silver_sactona]をデータフレームに変換しました")
    return df_sactona

# COMMAND ----------

# MAGIC %md
# MAGIC # SAS_Silver_join

# COMMAND ----------

def SAS_Silver_join(df_sap,df_sactona):
    print("データフレームをユニオンしています")
    df_union = df_sap.unionByName(df_sactona, allowMissingColumns=True)
    print("データフレームのユニオンが完了しました")
    return df_union

# COMMAND ----------

# MAGIC %md
# MAGIC # SAS_silver_delete

# COMMAND ----------

def SAS_silver_delete(table_name):
    if spark.catalog.tableExists(table_name):
        print("テーブルの既存データを削除します")
        df = spark.sql(f"""
          DELETE FROM {table_name}
        """)
        print("true:テーブルの既存データを削除しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # SAS_silver_create

# COMMAND ----------

def SAS_silver_create(table_name):	
 if not spark.catalog.tableExists(table_name):
  print("テーブルが存在しません、新規作成を行います")
  spark.sql(f"""
	 CREATE TABLE {table_name} (
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
# MAGIC # SAS_silver_incert

# COMMAND ----------

def SAS_silver_incert(df_union,table_name):
    print("テーブルにデータをインサートします")
    table_cols = spark.table(table_name).columns
    
    # df に存在しないカラムを NULL で追加
    for col in table_cols:
      if col not in df_union.columns:
        df_union = df_union.withColumn(col, lit(None))
 
    # テーブルカラム順に揃える
    df_aligned = df_union.select(table_cols)
 
    # テーブルに書き込み
    df_aligned.write.insertInto(table_name, overwrite=False)
    print("true:テーブルにデータのインサートが完了しました")

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import lit

table_name = "dev_first_usecase.silver.SILVER_sas"
try:
    df_sap = SAP_Silver_import()
    df_sactona = Sactona_Silver_import()
    df_union = SAS_Silver_join(df_sap,df_sactona)
    SAS_silver_delete(table_name)
    SAS_silver_create(table_name)
    SAS_silver_incert(df_union,table_name)
    print("Silver_SAS:TRUE")
except Exception as e:
    raise Exception(f"エラーが発生しました：{e}")
