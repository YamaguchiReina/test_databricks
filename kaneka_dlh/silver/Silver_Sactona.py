# Databricks notebook source
# MAGIC %md
# MAGIC # Sactona_bronze_import

# COMMAND ----------

def Sactona_bronze_import(bronze_tables):

  i = 0
  for table in bronze_tables:
     print(f"[{table}]からデータを取得しています……")
     df_bronze = spark.table(f"dev_first_usecase.bronze.{table}")
     df_bronze.createOrReplaceTempView(table)
     df_bronze = df_bronze.select([col(c).cast("string").alias(c) for c in df_bronze.columns])
     print(f"[{table}]をデータフレームに変換しました")
     i = i +1

  print(f"true:{i}個のデータフレームを取得しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sactona_silver_comp

# COMMAND ----------

def Sactona_silver_comp():
    print("データを補足しています……")
    df_comp = spark.sql(
     """
	 SELECT 1 AS plan_type ,
	 right(SACTONA.Entity,4) AS cost_center ,
	 right(SACTONA.Entity,6) AS charge_dept ,
	 right(SACTONA.Costcenter,6) AS allocation_dept ,
	 TRY_CAST(FLOOR(TRY_CAST(REPLACE(SACTONA.NUMDATA, ',', '') AS DECIMAL(18,2))) AS INT) AS budget_amount ,
	 0 AS actual_amount ,
	 SACTONA.InpCurr AS currency_code ,
	 0 AS reduced_budget_amount,
	 SACTONA.Category AS Category,
	 SACTONA.Time AS Time,
	 SACTONA.Entity AS Entity,
	 SACTONA.Account AS Account,
	 current_timestamp() AS registration_date
     
	 FROM BRONZE_TRN_Sactona AS SACTONA
	 WHERE SACTONA.Category = "BUDGET"
	 """
    )
    # df_comp = df_comp.select([col(c).cast("string").alias(c) for c in df_comp.columns])
    df_comp.createOrReplaceTempView("df_comp")
    print("true:データの補足が完了しました")
    return df_comp

# COMMAND ----------

# MAGIC %md
# MAGIC # Sactona_silver_join

# COMMAND ----------

def Sactona_silver_join(df_comp):
	print("マスタを結合しています……")
	df_join = spark.sql("""
	 SELECT 
	 df_comp.plan_type ,
	 df_comp.cost_center ,
	 df_comp.charge_dept ,
	 df_comp.allocation_dept ,
	 df_comp.budget_amount ,
	 df_comp.actual_amount ,
	 df_comp.currency_code ,
	 df_comp.reduced_budget_amount,
	 Category.DESCRIPTION1 AS plan_type_name ,
	 Time.YEARNUM AS fiscal_year ,
	 Time.MONTHNUM AS period ,
	 Time.STYLE AS record_month ,
	 Costcenter.DESCRIPTION1 AS cost_center_name ,
	 SAP.site_id AS site_id ,
	 SAP.site_id_name AS site_id_name ,
	 SAP.fin_cost_type AS fin_cost_type ,
	 SAP.fin_cost_type_name AS fin_cost_type_name ,
	 SAP.admin_cost_type AS admin_cost_type ,
	 SAP.admin_cost_type_name AS admin_cost_type_name ,
	 SAP.division_id AS division_id ,
	 SAP.division_id_name AS division_id_name ,
	 SAP.project AS industry_code ,
	 SAP.project_name AS industry_code_name ,
	 SAP.org_unit AS org_unit ,
	 SAP.org_unit_name AS org_unit_name ,
	 Expense.expense_type AS expense_type ,
	 Expense.expense_type_name AS expense_type_name ,
	 Account.S4_SAPCode1 AS account ,
	 Account.DisplayName AS account_name ,
	 Account.DESCRIPTION1 AS account_detail_name,
	 Costcenter.DESCRIPTION1 AS charge_dept_name ,
	 Account.S4_SAPCode1 AS mgmt_account_item,
  	 current_timestamp() AS registration_date,	
	 CASE WHEN right(record_month,2) BETWEEN '04' AND '06' THEN '第１四半期' WHEN right(record_month,2) BETWEEN '07' AND '09' THEN '第２四半期' WHEN right(record_month,2) BETWEEN '10' AND '12' THEN '第３四半期' WHEN right(record_month,2) BETWEEN '01' AND '03' THEN '第４四半期' END AS j_fiscal_quarter,
	 CASE WHEN right(record_month,2) BETWEEN '04' AND '06' THEN '1' WHEN right(record_month,2) BETWEEN '07' AND '09' THEN '2' WHEN right(record_month,2) BETWEEN '10' AND '12' THEN '3' WHEN right(record_month,2) BETWEEN '01' AND '03' THEN '4' END AS nj_fiscal_quarter,
	 right(period,1) AS fiscal_month,
	 left(record_month,4) AS year,
	 right(record_month,1) AS month,
	 fiscal_year AS record_year,
	 CASE WHEN right(record_month,2) BETWEEN '04' AND '09' THEN '1:上期' WHEN (right(record_month,2) BETWEEN '10' AND '12')OR (right(record_month,2) BETWEEN '01' AND '03') THEN '2:下期' END AS fiscal_half,
	 CASE WHEN right(record_month,2) BETWEEN '04' AND '09' THEN '1' WHEN (right(record_month,2) BETWEEN '10' AND '12')OR (right(record_month,2) BETWEEN '01' AND '03') THEN '2' END AS n_fiscal_half,
	 0 AS transportation_cost,
	 0 AS transfer_freight_storage_charge,
	 0 AS freight_storage_charge,
	 0 AS plant_site_handling_fee

	 FROM df_comp
	 LEFT JOIN BRONZE_MST_account AS Account
	 ON df_comp.Account = account.ID
	 LEFT JOIN BRONZE_MST_category AS Category
	 ON df_comp.Category = Category.ID
	 LEFT JOIN BRONZE_MST_time AS Time
	 ON df_comp.Time = Time.ID
	 LEFT JOIN BRONZE_MST_costcenter AS Costcenter
	 ON df_comp.Entity = Costcenter.ID
	 LEFT JOIN BRONZE_MST_entity AS Entity
	 ON df_comp.Entity = Entity.ID
	 LEFT JOIN BRONZE_MST_expensetype AS Expense
	 ON Account.S4_SAPCode1 = Expense.account
	 LEFT JOIN BRONZE_MST_SAP AS SAP
	 ON right(df_comp.Entity,4) = SAP.cost_center
	 WHERE Account.DisplayName IS NOT NULL
	 """
	)
	print("true:マスタの結合が完了しました")
	return df_join


# COMMAND ----------

# MAGIC %md
# MAGIC # Sactona_silver_delete

# COMMAND ----------

def Sactona_silver_delete():
  if spark.catalog.tableExists('dev_first_usecase.silver.SILVER_Sactona'):
    print("テーブルの既存データを削除します")
    spark.sql(f"""
      DELETE FROM dev_first_usecase.silver.SILVER_sactona
    """)
    print(f"true:テーブルの既存データを削除しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sactona_silver_create

# COMMAND ----------

def Sactona_silver_create():
	if not spark.catalog.tableExists('dev_first_usecase.silver.SILVER_Sactona'):
	 print("テーブルが存在しません、新規作成を行います")
	 spark.sql("""
	 CREATE OR REPLACE TABLE dev_first_usecase.silver.silver_sactona (
	  fiscal_year	STRING	,
      period STRING	,
      record_month	STRING	,
      expense_type	STRING	,
      expense_type_name	STRING	,
      account	STRING	,
      account_name	STRING	,
      account_detail	STRING	,
      account_detail_name	STRING	,
      plan_type	STRING	,
      plan_type_name	STRING	,
      mgmt_account_item	STRING	,
      admin_cost_type	STRING	,
      admin_cost_type_name	STRING	,
      fin_cost_type	STRING	,
      fin_cost_type_name	STRING	,
      document_tp	STRING	,
      document_type	STRING	,
      slip_number	STRING	,
      line_number	STRING	,
      post_date	STRING	,
      pay_date	STRING	,
      issue_section	STRING	,
      issue_section_name	STRING	,
      charge_dept	STRING	,
      charge_dept_name	STRING	,
      detail_text	STRING	,
      sort_key	STRING	,
      header_text	STRING	,
      allocation_dept	STRING	,
      vendor_code	STRING	,
      vendor_name_full_width	STRING	,
      vendor_name_half_width	STRING	,
      site_id	STRING	,
      site_id_name	STRING	,
      division_id	STRING	,
      division_id_name	STRING	,
      industry_code	STRING	,
      industry_code_name	STRING	,
      org_unit	STRING	,
      org_unit_name	STRING	,
      cost_center	STRING	,
      cost_center_name	STRING	,
      wf_request_id	STRING	,
      business_traveler	STRING	,
      business_trip_dest	STRING	,
      business_trip_date	STRING	,
      user_name	STRING	,
      store_name	STRING	,
      usage_date	STRING	,
      entertainment_party	STRING	,
      currency_type	STRING	,
      actual_foreign_amount	STRING	,
      inout_cd	STRING	,
      product_type	STRING	,
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
      user_full_name	STRING	,
      record_month_min	STRING	,
      record_month_max	STRING	,
      gr_admin_cd	STRING	,
      gr_admin_name	STRING	,
      user_name2	STRING	,
      seq_no	STRING	,
      extract_time	STRING	,
      extract_date	STRING	,
      reduced_budget_amount	STRING	,
      tax_amount	STRING	,
      tax_code	STRING	,
      currency_code	STRING	,
      registration_date	STRING	,
      j_fiscal_quarter	STRING	,
      nj_fiscal_quarter	STRING	,
      fiscal_month	STRING	,
      year	STRING	,
      month	STRING	,
      record_year	STRING	,
      fiscal_half	STRING	,
      n_fiscal_half	STRING	,
      transportation_cost	STRING	,
      transfer_freight_storage_charge	STRING	,
      freight_storage_charge	STRING	,
      plant_site_handling_fee	STRING	,
      actual_amount NUMERIC(15,5),
      budget_amount NUMERIC(15,5)	
	)
	""")
	print("true:テーブル作成が完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sactona_silver_incert

# COMMAND ----------

def Sactona_silver_incert(df_join):
    print("テーブルにデータをインサートします")
 
    table_cols = spark.table("dev_first_usecase.silver.silver_sactona").columns
    
    # df に存在しないカラムを NULL で追加
    for col in table_cols:
      if col not in df_join.columns:
        df_join = df_join.withColumn(col, lit(None))
 
    # テーブルカラム順に揃える
    df_aligned = df_join.select(table_cols)
 
    # テーブルに書き込み
    df_aligned.write.insertInto("dev_first_usecase.silver.silver_sactona", overwrite=False)
    print("true:テーブルにデータのインサートが完了しました")

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import lit
bronze_tables=["BRONZE_MST_account","BRONZE_MST_category","BRONZE_MST_costcenter","BRONZE_MST_entity","BRONZE_MST_time","BRONZE_MST_product","BRONZE_MST_expensetype","BRONZE_TRN_Sactona","BRONZE_MST_SAP"]
try:
    Sactona_bronze_import(bronze_tables)
    comp = Sactona_silver_comp()
    join = Sactona_silver_join(comp)
    Sactona_silver_delete()
    Sactona_silver_create()
    Sactona_silver_incert(join)
    print("Silver_Sactona:TRUE")
except Exception as e:
    raise Exception(f"エラーが発生しました：{e}")
