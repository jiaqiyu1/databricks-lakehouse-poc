# ==========================================================
# Notebook: 02_transform_silver.py
# Silver Layer - Business Rule Standardisation & Data Quality
#
# Purpose:
# - Clean and standardise Bronze data
# - Apply business rules
# - Enforce data types
# - Handle nulls and invalid values
# - Prepare clean, trusted data for Gold layer
#
# Design principles:
# - No aggregations
# - No dimensional modelling
# - Deterministic and repeatable transformations
# ==========================================================

import pyspark.sql.functions as F

# -----------------------------
# CONFIGURATION
# -----------------------------
CATALOG_SCHEMA = "default"

# Input Bronze table
bronze_table_name = f"{CATALOG_SCHEMA}.bronze_funding_contracting_data"

# Output Silver table
silver_table_name = f"{CATALOG_SCHEMA}.silver_funding_contracting_data"


# ==========================================================
# Step 1 — Load Bronze Layer
# ==========================================================
df = spark.read.table(bronze_table_name)
print(f"✅ Loaded Bronze table: {bronze_table_name}")


# ==========================================================
# Step 2 — Global Data Cleansing
#   - Trim all string columns
#   - Convert empty strings to NULL
# ==========================================================
df_cleaned = df.select([
    (
        F.when(F.trim(F.col(c)) == "", None)
         .otherwise(F.trim(F.col(c)))
         .alias(c)
        if dict(df.dtypes)[c] == "string"
        else F.col(c)
    )
    for c in df.columns
])

print("🧹 Applied global string trimming and empty-to-null conversion")


# ==========================================================
# Step 3 — Apply Business Rules & Type Standardisation
# ==========================================================
df_silver = (
    df_cleaned

    # ------------------------------------------
    # Business Key Standardisation
    # ------------------------------------------
    # Provider_ID should be treated as a business key (string)
    .withColumn("Provider_ID", F.col("Provider_ID").cast("string"))

    # ------------------------------------------
    # Business Rules
    # ------------------------------------------
    # Contract number must exist
    .filter(F.col("Contract_Number").isNotNull())

    # Contracted service total must be larger than $5
    .filter(F.col("Contracted_Service_Total") >= 5)

    # ------------------------------------------
    # Date Normalisation
    # ------------------------------------------
    .withColumn("Contract_Start_Date", F.to_date("Contract_Start_Date"))
    .withColumn("Contract_End_Date", F.to_date("Contract_End_Date"))
    .withColumn("Contract_Sign-Off_Date", F.to_date("Contract_Sign-Off_Date"))
    .withColumn("Funding_Item_Start_Date", F.to_date("Funding_Item_Start_Date"))
    .withColumn("Funding_Item_End_Date", F.to_date("Funding_Item_End_Date"))
    .withColumn("Last_Return_Submission_Date", F.to_date("Last_Return_Submission_Date"))
)
print(f"📊 Row count after Silver transformations: {df_silver.count()}")


# ==========================================================
# Step 4 — DROP & Recreate Silver Table
#   Reason:
#   - Ensures schema consistency
#   - Simplifies iteration during POC
# ==========================================================
spark.sql(f"DROP TABLE IF EXISTS {silver_table_name}")
print(f"🧹 Dropped existing Silver table if existed: {silver_table_name}")


# ==========================================================
# Step 5 — Write Silver Delta Table
# ==========================================================
(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table_name)
)

print("=========================================================")
print(f"✅ Silver table successfully created: {silver_table_name}")
print(f"📊 Final row count: {spark.table(silver_table_name).count()}")
print("=========================================================")

# Optional preview
display(spark.table(silver_table_name).limit(20))
