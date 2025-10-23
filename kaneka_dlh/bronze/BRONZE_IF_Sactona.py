# Databricks notebook source
# MAGIC %md
# MAGIC # Sactona_csv_import

# COMMAND ----------

def Sactona_csv_import(code):

  print("CSVを取得しています……")

  # CSVファイルのパスをリストに格納
  csv_file = ["abfss://data@dbxjpedevst001.dfs.core.windows.net/Phase1/Sactona/Sactona_経費見込データ.csv"]

  # 各CSVファイルを読み込み、DataFrameを作成
  df = spark.read.csv(csv_file, header=True, inferSchema=False)

  # 一時ビューを作成
  return df.createOrReplaceTempView(code)

# COMMAND ----------

# MAGIC %md
# MAGIC # Sactona_bronze_delete

# COMMAND ----------

def Sactona_bronze_delete(table_name):
    if spark.catalog.tableExists(table_name):
        print("テーブルの既存データを削除します")
        df = spark.sql(f"""
            DELETE FROM {table_name}
        """)
        print("true:テーブルの既存データを削除しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sactona_bronze_create

# COMMAND ----------

def Sactona_bronze_create(table_name):
    if not spark.catalog.tableExists(table_name):
        print("テーブルが存在しません、新規作成を行います")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
            Category STRING,
            DataSrc STRING,
            Entity STRING,
            Time STRING,
            Product STRING,
            Account STRING,
            InterCo STRING,
            InpCurr STRING,
            RptCurr STRING,
            CostCenter STRING,
            NUMDATA STRING
            )
        """)
        print("true:テーブル作成が完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sactona_bronze_incert

# COMMAND ----------

def Sactona_bronze_incert(code,table_name):
    print("テーブルにデータをインサートします")
    df = spark.sql(f"""
        INSERT INTO {table_name}
        SELECT *
        FROM {code};
    """)
    print("true:テーブルにデータのインサートが完了しました")

# COMMAND ----------

table_name = "dev_first_usecase.bronze.BRONZE_TRN_Sactona"
code = "sactona"
try:
    result = Sactona_csv_import(code)
    print("true:CSVを取得しました")
    Sactona_bronze_delete(table_name)
    Sactona_bronze_create(table_name)
    Sactona_bronze_incert(code,table_name)
    print("BRONZE_IF_Sactona:TRUE")
except Exception as e:
    raise Exception(f"エラーが発生しました：{e}")
