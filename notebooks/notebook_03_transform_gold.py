
# ==========================================================
# Notebook: 03_transform_gold.py
# Gold Layer – Dimensional Modelling (NO SCD2)
#
# Purpose:
# - Transform clean Silver data into analytics-ready structures
# - Build star schema (Dimensions + Fact)
# - Assign surrogate keys to dimensions
# - Prepare trusted, consumable datasets for reporting and analytics
# - Serve downstream tools (e.g. Power BI, data sharing, insights)
#
# Scope:
# - Dimension tables represent the current state (NO SCD2)
# - Fact table captures measurable values and references dimensions
# - Only ONE date foreign key is used in the fact table:
#     start_of_fy_sk (start of financial year – 1 July)
#
# Design principles:
# - Star schema modelling (dimensional modelling)
# - Business keys remain in dimensions; facts use surrogate keys
# - No business logic duplication from Silver layer
# - No data cleansing (assumed completed in Silver)
# - Deterministic, repeatable and idempotent transformations
# - Optimised for analytics, not for raw ingestion
#
# Notes:
# - Fiscal calendar follows NZ standard (July–June)
# - This implementation uses Hive metastore (default schema),
#   not Unity Catalog
# ==========================================================


import pyspark.sql.functions as F

CATALOG_SCHEMA = "default"

# Silver input
silver_table = f"{CATALOG_SCHEMA}.silver_funding_contracting_data"

# Output tables
dim_provider_table = f"{CATALOG_SCHEMA}.dim_provider"
dim_contract_table = f"{CATALOG_SCHEMA}.dim_contract"
dim_service_table = f"{CATALOG_SCHEMA}.dim_service"
dim_funding_item_table = f"{CATALOG_SCHEMA}.dim_funding_item"
fact_funding_table = f"{CATALOG_SCHEMA}.fact_funding_values"

# Load Silver data
df = spark.table(silver_table)

# ===========================================================
# DIM PROVIDER
# ===========================================================
dim_provider = (
    df.select(
        "Provider_ID",
        "Provider_Name",
        "Provider_Admin_Ethnicity",
        "Provider_Area",
        "Provider_Approval_Level"
    )
    .dropDuplicates(["Provider_ID"])
    .withColumn("provider_sk", F.monotonically_increasing_id())
)

dim_provider.write.format("delta").mode("overwrite").saveAsTable(dim_provider_table)

# ===========================================================
# DIM CONTRACT
# ===========================================================
dim_contract = (
    df.select(
        "Contract_Number",
        "Contract_Name",
        "Contract_Status",
        "Contract_Start_Date",
        "Contract_End_Date",
        "Contract_Sign-Off_Date",
        "Contract_Manager_Surname",
        "Contract_Manager_First_Name",
        "Contract_Manager_Name",
        "Region",
        "Contract_Service_Code"
    )
    .dropDuplicates(["Contract_Number"])
    .withColumn("contract_sk", F.monotonically_increasing_id())
)

dim_contract.write.format("delta").mode("overwrite").saveAsTable(dim_contract_table)

# ===========================================================
# DIM SERVICE
# ===========================================================
dim_service = (
    df.select(
        "Service_Type_Code",
        "Service",
        "Provider_Service_Description",
        "Service_Delivery_Areas",
        "Service_Categorisation",
        "Programme",
        "Output_Class",
        "Service_Approval_Level"
    )
    .dropDuplicates(["Service_Type_Code"])
    .withColumn("service_sk", F.monotonically_increasing_id())
)

dim_service.write.format("delta").mode("overwrite").saveAsTable(dim_service_table)

# ===========================================================
# DIM FUNDING ITEM
# ===========================================================
dim_funding_item = (
    df.select(
        "Funding_Item_ID",
        "Funding_Item_Status",
        "Funding_Item_Start_Date",
        "Funding_Item_End_Date",
        "Funding_Item_Comment"
    )
    .dropDuplicates(["Funding_Item_ID"])
    .withColumn("funding_item_sk", F.monotonically_increasing_id())
)

dim_funding_item.write.format("delta").mode("overwrite").saveAsTable(dim_funding_item_table)

# ===========================================================
# FACT TABLE
# ===========================================================

dim_date_df = spark.table(f"{CATALOG_SCHEMA}.dim_date")

fact = df

# Join provider → provider_sk
fact = fact.join(dim_provider, on="Provider_ID", how="left")

# Join contract → contract_sk
fact = fact.join(dim_contract, on="Contract_Number", how="left")

# Join service → service_sk
fact = fact.join(dim_service, on="Service_Type_Code", how="left")

# Join funding item → funding_item_sk
fact = fact.join(dim_funding_item, on="Funding_Item_ID", how="left")

# ===========================================================
# Start of Financial Year = July 1 (FY end year - 1)
# ===========================================================

fact = fact.withColumn(
    "fy_end_year",
    F.regexp_extract(F.col("Financial_Year"), r"(\d{4})$", 1).cast("int")
)

fact = fact.withColumn("fy_start_year", F.col("fy_end_year") - 1)

fact = fact.withColumn(
    "start_of_financial_year",
    F.to_date(
        F.concat_ws("-", F.col("fy_start_year"), F.lit("07"), F.lit("01")),
        "yyyy-MM-dd"
    )
)

fact = fact.drop("fy_end_year", "fy_start_year")

# Map to dim_date → start_of_fy_sk
fact = fact.join(
    dim_date_df,
    fact["start_of_financial_year"] == dim_date_df["FullDate"],
    "left"
).withColumnRenamed("DateKey", "start_of_fy_sk")

# ===========================================================
# Final Fact Table
# ===========================================================
fact_funding = fact.select(
    "provider_sk",
    "contract_sk",
    "service_sk",
    "funding_item_sk",
    "start_of_fy_sk",   # ONLY date foreign key

    # Measures
    "Contracted_Service_Volume",
    "Contracted_Service_Rate",
    "Contracted_Service_Total",
    "Scheduled_Amount",
    "Actual_Amount_Paid",

    # Metadata
    F.current_timestamp().alias("ingested_ts"),
    F.lit("silver_funding_contracting_data").alias("source_table")
)

fact_funding.write.format("delta").mode("overwrite").saveAsTable(fact_funding_table)

print("=========================================================")
print("✅ Gold Layer successfully created.")
print("=========================================================")

display(spark.table(fact_funding_table).limit(20))