# Databricks notebook source
# MAGIC %md
# MAGIC # SactonaM_csv_import

# COMMAND ----------

def SactonaM_csv_import(csv_files):

  df_list = []
  i = 0
  for csv in csv_files:
     print(f"[{csv}]からCSVを取得しています……")
     df = spark.read.option("header", True).csv(csv)
     df_list.append(df)
     print(f"[{csv}]をリストに追加しました")
     i = i +1
  return df_list

# COMMAND ----------

# MAGIC %md
# MAGIC # SactonaM_bronze_delete

# COMMAND ----------

def SactonaM_bronze_delete(tables):
  i = 0
  for table in tables:
    if spark.catalog.tableExists(table):
      print(f"[{table}]の既存データを削除します")
      spark.sql(f"""
        DELETE FROM {table}
      """)
      i = i + 1
  if i > 0:
    print(f"true:{i}個のテーブルを削除しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # SactonaM_bronze_create

# COMMAND ----------

def SactonaM_bronze_create(df_list, tables):

  i = 0
  for df , table in zip(df_list, tables):
    if not spark.catalog.tableExists(table):
      print(f"[{table}]を作成しています……")
      df.write.format("delta").mode("overwrite").saveAsTable(table)
      print(f"[{table}]を作成しました")
      i = i + 1
  if i > 0:
    print(f"true:{i}個のテーブルを作成しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # SactonaM_bronze_incert

# COMMAND ----------

def SactonaM_bronze_incert(df_list, tables):
  i = 0
  for df , table in zip(df_list, tables):
    print(f"[{table}]を更新しています……")
    df.write.format("delta").mode("overwrite").saveAsTable(table)
    print(f"[{table}]を更新しました")
    i = i + 1
  if i > 0:
    print(f"true:{i}個のテーブルを更新しました")

# COMMAND ----------

csv_files=["abfss://data@dbxjpedevst001.dfs.core.windows.net/Phase1/Sactona/ExportDimensionMember_Account.csv","abfss://data@dbxjpedevst001.dfs.core.windows.net/Phase1/Sactona/ExportDimensionMember_Category.csv","abfss://data@dbxjpedevst001.dfs.core.windows.net/Phase1/Sactona/ExportDimensionMember_CostCenter.csv","abfss://data@dbxjpedevst001.dfs.core.windows.net/Phase1/Sactona/ExportDimensionMember_Entity.csv","abfss://data@dbxjpedevst001.dfs.core.windows.net/Phase1/Sactona/ExportDimensionMember_Time.csv","abfss://data@dbxjpedevst001.dfs.core.windows.net/Phase1/Sactona/ExportDimensionMember_Product.csv","abfss://data@dbxjpedevst001.dfs.core.windows.net/Phase1/Sactona/expense_type.csv"]
tables=["dev_first_usecase.bronze.BRONZE_MST_account","dev_first_usecase.bronze.BRONZE_MST_category","dev_first_usecase.bronze.BRONZE_MST_costcenter","dev_first_usecase.bronze.BRONZE_MST_entity","dev_first_usecase.bronze.BRONZE_MST_time","dev_first_usecase.bronze.BRONZE_MST_product","dev_first_usecase.bronze.BRONZE_MST_expensetype"]

try:
    result = SactonaM_csv_import(csv_files)
    print(f"true:データフレームを取得しました")
    SactonaM_bronze_delete(tables)
    SactonaM_bronze_create(result, tables)
    SactonaM_bronze_incert(result, tables)
    print("BRONZE_MST_Sactona:TRUE")
except Exception as e:
    raise Exception(f"エラーが発生しました：{e}")
