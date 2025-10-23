# Databricks notebook source
# MAGIC %md
# MAGIC # SAPM_csv_import

# COMMAND ----------

def SAPM_csv_import(code):

  print("CSVを取得しています……")

  # CSVファイルのパスをリストに格納
  csv_file = ["abfss://data@dbxjpedevst001.dfs.core.windows.net/Phase1/SAP/SAS部門マスタ.csv"]

  # 各CSVファイルを読み込み、DataFrameを作成
  df = spark.read.csv(csv_file, header=True, inferSchema=False)

  # 一時ビューを作成
  return df.createOrReplaceTempView(code)


# COMMAND ----------

# MAGIC %md
# MAGIC # SAPM_bronze_delete

# COMMAND ----------

def SAPM_bronze_delete(table_name):
    if spark.catalog.tableExists(table_name):
        print("テーブルの既存データを削除します")
        df = spark.sql(f"""
          DELETE FROM {table_name}
        """)
        print("true:テーブルの既存データを削除しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # SAPM_bronze_create

# COMMAND ----------

def SAPM_bronze_create(table_name):
  if not spark.catalog.tableExists(table_name):
    print("テーブルが存在しません、新規作成を行います")
    spark.sql(f"""
      CREATE TABLE {table_name} (
      cost_center	STRING	,
      category	STRING	,
      category_name	STRING	,
      org_unit	STRING	,
      org_unit_name	STRING	,
      project	STRING	,
      project_name	STRING	,
      industry_code	STRING	,
      industry_code_name	STRING	,
      area	STRING	,
      area_name	STRING	,
      section	STRING	,
      section_name	STRING	,
      division_id	STRING	,
      division_id_name	STRING	,
      site_id	STRING	,
      site_id_name	STRING	,
      dept_short	STRING	,
      dept_long	STRING	,
      fin_cost_type	STRING	,
      fin_cost_type_name	STRING	,
      admin_cost_type	STRING	,
      admin_cost_type_name	STRING	,
      cost_center_g_level1	STRING	,
      cost_center_g_level1_name	STRING	,
      cost_center_g_level2	STRING	,
      cost_center_g_level2_name	STRING	,
      cost_center_g_level3	STRING	,
      cost_center_g_level3_name	STRING	,
      cost_center_g_level4	STRING	,
      cost_center_g_level4_name	STRING	,
      cost_center_g_level5	STRING	,
      cost_center_g_level5_name	STRING	,
      cost_center_g_level6	STRING	,
      cost_center_g_level6_name	STRING	,
      extract_date	STRING	,
      extract_time	STRING
    )
    """)
    print("true:テーブル作成が完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # SAPM_bronze_incert

# COMMAND ----------

def SAPM_bronze_incert(code,table_name):
    print("テーブルにデータをインサートします")
    df = spark.sql(f"""
        INSERT INTO dev_first_usecase.bronze.BRONZE_MST_SAP
        SELECT *
        FROM {code};
    """)
    print("true:テーブルにデータのインサートが完了しました")

# COMMAND ----------

table_name = "dev_first_usecase.bronze.BRONZE_MST_SAP"
code = "m_sap"
try:
    result = SAPM_csv_import(code)
    print("true:CSVを取得しました")
    SAPM_bronze_delete(table_name)
    SAPM_bronze_create(table_name)
    SAPM_bronze_incert(code,table_name)
    print("BRONZE_MST_SAP:TRUE")
except Exception as e:
    raise Exception(f"エラーが発生しました：{e}")
