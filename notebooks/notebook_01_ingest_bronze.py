
# ==========================================================
# Notebook: 01_ingest_bronze.py
# Bronze Layer - Raw Data Ingestion
#
# Purpose:
# - Ingest raw source data into Bronze layer
# - Standardise column names only
# - Do NOT apply business rules
# - Recreate table every run (POC strategy)
# ==========================================================

import pyspark.sql.functions as F

# -----------------------------
# CONFIGURATION
# -----------------------------
CATALOG_SCHEMA = "default"

# Source table (mock data already uploaded as Delta table)
source_table_name = f"{CATALOG_SCHEMA}.fac_mock_data"

# Target Bronze table
bronze_table_name = f"{CATALOG_SCHEMA}.bronze_funding_contracting_data"


# ==========================================================
# Step 1 — Read Source Table
# ==========================================================
try:
    df = spark.read.table(source_table_name)
    print(f"✅ Successfully read source table: {source_table_name}")
except Exception as e:
    raise Exception(
        f"❌ Source table not found: {source_table_name}\n"
        f"Error: {e}"
    )


# ==========================================================
# Step 2 — Standardise Column Names
#   - Replace spaces and special characters
#   - Keep naming consistent for downstream layers
# ==========================================================
def clean_column(name: str) -> str:
    return (
        name.strip()
        .replace(" ", "_")
        .replace(",", "")
        .replace(";", "")
        .replace("{", "")
        .replace("}", "")
        .replace("(", "")
        .replace(")", "")
        .replace("\n", "")
        .replace("\t", "")
        .replace("=", "")
    )

cleaned_columns = [clean_column(c) for c in df.columns]
df_clean = df.toDF(*cleaned_columns)

print("🔧 Column names standardised for Bronze layer")


# ==========================================================
# Step 3 — DROP existing Bronze table (POC strategy)
#   Reason:
#   - Mock data schema changes frequently
#   - Avoid Delta schema merge conflicts
#   - Keep Bronze deterministic and reproducible
# ==========================================================
spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name}")
print(f"🧹 Dropped existing Bronze table if existed: {bronze_table_name}")


# ==========================================================
# Step 4 — Write Bronze Delta Table
# ==========================================================
(
    df_clean.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(bronze_table_name)
)

print("=========================================================")
print(f"✅ Bronze table successfully created: {bronze_table_name}")
print(f"📊 Row count: {spark.table(bronze_table_name).count()}")
print("=========================================================")

# Optional preview
display(spark.table(bronze_table_name).limit(20))