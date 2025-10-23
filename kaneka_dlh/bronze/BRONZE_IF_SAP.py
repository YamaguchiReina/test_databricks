# Databricks notebook source
# MAGIC %md
# MAGIC # SAP_csv_import

# COMMAND ----------

def SAP_csv_import(code):
    print("CSVを取得しています……")
    # 各CSVファイルを読み込み、DataFrameを作成
    df1 = spark.read.csv(
        "abfss://data@dbxjpedevst001.dfs.core.windows.net/Phase1/SAP/KM1001F_TB0056J_202503.csv",
        header=True,
        inferSchema=False,
    )
    df2 = spark.read.csv(
        "abfss://data@dbxjpedevst001.dfs.core.windows.net/Phase1/SAP/KM1001F_TB0056J_202504-202506.csv",
        header=True,
        inferSchema=False,
    )

    df_union = df1.unionByName(df2, allowMissingColumns=True)
    return df_union.createOrReplaceTempView(code)

# COMMAND ----------

# MAGIC %md
# MAGIC # SAP_bronze_delete

# COMMAND ----------

def SAP_bronze_delete(table_name):
    if spark.catalog.tableExists(table_name):
        print("テーブルの既存データを削除します")
        df = spark.sql(f"""
            DELETE FROM {table_name}
        """)
        print("true:テーブルの既存データを削除しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # SAP_bronze_create

# COMMAND ----------

def SAP_bronze_create(table_name):
    if not spark.catalog.tableExists(table_name):
        print("テーブルが存在しません、新規作成を行います")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
            plan_type string,
            plan_type_name string,     
            fiscal_year string,
            period string,
            record_month string,
            cost_center string,
            cost_center_name string,
            site_id string,
            site_id_name string,
            fin_cost_type string,
            fin_cost_type_name string,
            admin_cost_type string,
            admin_cost_type_name string,
            division_id string,
            division_id_name string,
            industry_code string,
            industry_code_name string,
            org_unit string,
            org_unit_name string,
            cost_center_g_level1 string,
            cost_center_g_level1_name string,
            cost_center_g_level2 string,
            cost_center_g_level2_name string,
            cost_center_g_level3 string,
            cost_center_g_level3_name string,
            cost_center_g_level4 string,
            cost_center_g_level4_name string,
            cost_center_g_level5 string,
            cost_center_g_level5_name string,
            cost_center_g_level6 string,
            cost_center_g_level6_name string,
            expense_type string,
            expense_type_name string,
            account string,
            account_name string,
            account_detail string,
            account_detail_name string,
            charge_dept string,
            charge_dept_name string,
            issue_section string,
            issue_section_name string,
            post_date string,
            pay_date string,
            sort_key string,
            vendor_code string,
            vendor_name_full_width string,
            vendor_name_half_width string,
            header_text string,
            detail_text string,
            mgmt_account_item string,
            allocation_dept string,
            entertainment_party string,
            user_name string,
            store_name string,
            usage_date string,
            business_trip_dest string,
            business_trip_date string,
            business_traveler string,
            slip_number string,
            line_number string,
            wf_request_id string,
            document_type string,
            extract_date string,
            extract_time string,
            budget_amount string,
            actual_amount string,
            currency_code string,
            actual_foreign_amount string,
            currency_type string,
            reduced_budget_amount string,
            seq_no string,
            document_tp string,
            tax_code string,
            tax_amount string,
            inout_cd string,
            product_type string,
            registration_date string
            )
            """
        )
        print("true:テーブル作成が完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC # SAP_bronze_incert

# COMMAND ----------

def SAP_bronze_incert(code,table_name):
    print("テーブルにデータをインサートします")
    spark.sql(f"""
        INSERT INTO {table_name}
        SELECT *
        FROM {code};
    """)
    print("true:テーブルにデータのインサートが完了しました")

# COMMAND ----------

table_name = "dev_first_usecase.bronze.BRONZE_TRN_SAP"
code = "sap_if"
try:
    SAP_csv_import(code)
    print("true:CSVを取得しました")
    SAP_bronze_delete(table_name)
    SAP_bronze_create(table_name)
    SAP_bronze_incert(code,table_name)
    print("BRONZE_IF_SAP:TRUE")
except Exception as e:
    raise Exception(f"エラーが発生しました：{e}")
