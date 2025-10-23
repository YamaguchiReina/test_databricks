# Databricks notebook source
# MAGIC %md
# MAGIC # SAS_Silver_import

# COMMAND ----------

def SAS_Silver_import():
    print("[dev_first_usecase.silver.SILVER_sas]からデータを取得しています……")
    df_silver = spark.table("dev_first_usecase.silver.SILVER_sas")
    df_silver.createOrReplaceTempView("df_silver")
    df_silver = df_silver.select([col(c).cast("string").alias(c) for c in df_silver.columns])
    print(f"true:[dev_first_usecase.silver.SILVER_sas]をデータフレームに変換しました")
    return df_silver

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold_delete

# COMMAND ----------

def Gold_delete(table_name):
    if spark.catalog.tableExists(table_name):
        print("テーブルの既存データを削除します")
        df = spark.sql(f"""
            DELETE FROM {table_name}
        """)
    print("true:テーブルの既存データを削除しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold_create

# COMMAND ----------

def Gold_create(table_name):	
 if not spark.catalog.tableExists(table_name):
  print("テーブルが存在しません、新規作成を行います")
  spark.sql(f"""
  CREATE OR REPLACE TABLE {table_name}(
  record_month STRING,
  expense_type_name STRING,      
  account_name STRING,
  account_detail_name STRING,
  cost_center STRING,
  budget_amount NUMERIC(15,5),
  actual_amount NUMERIC(15,5)       
  )
  """)
  print("true:テーブル作成が完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold_incert

# COMMAND ----------

def Gold_incert(df_silver,table_name):
  df_silver.createOrReplaceTempView("df_silver")
  print("テーブルにデータをインサートします")
  spark.sql(f"""
    INSERT INTO dev_first_usecase.gold.GOLD
    SELECT 
    silver.record_month,
    silver.expense_type_name,
    silver.account_name,
    silver.account_detail_name,
    silver.cost_center,
    silver.budget_amount,
    silver.actual_amount

    FROM df_silver as silver
    """)
  print("true:テーブルにデータのインサートが完了しました")

# COMMAND ----------

from pyspark.sql.functions import col

table_name = "dev_first_usecase.gold.GOLD"
try:
    df_silver = SAS_Silver_import()
    Gold_delete(table_name)
    Gold_create(table_name)
    Gold_incert(df_silver,table_name)
    print("GOLD:TRUE")
except Exception as e:
    raise Exception(f"エラーが発生しました：{e}")
