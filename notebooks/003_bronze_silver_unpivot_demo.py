# Databricks notebook source
# MAGIC %md
# MAGIC # The Zen of the Bronze Layer: Embracing Schema Chaos
# MAGIC
# MAGIC This notebook demonstrates the unpivot pattern for vendor data integration. The core insight: treat column names as data, not schema.
# MAGIC
# MAGIC ## The Problem
# MAGIC
# MAGIC Traditional approaches treat CSV column names as schema constraints. When Vendor A calls pH "ph" and Vendor B calls it "acidity", you write mapping logic. When typos appear, you add fuzzy matching. Each variation becomes a code problem requiring a code solution.
# MAGIC
# MAGIC ## The Solution: Unpivot to Long Format
# MAGIC
# MAGIC The unpivot pattern is a variant of the Entity-Attribute-Value (EAV) model. We store key-value pairs with position metadata. Column names become data values we can query, filter, and join against.
# MAGIC
# MAGIC **Bronze Layer** (this notebook):
# MAGIC - Parse and unpivot all vendor CSVs into long format
# MAGIC - Fixed schema: `row_index`, `column_index`, `lab_provided_attribute`, `lab_provided_value`
# MAGIC - Vendor-agnostic; same code processes every file
# MAGIC
# MAGIC **Silver Layer** (this notebook):
# MAGIC - Join with mapping tables to standardize column names
# MAGIC - Configuration over code: vendor differences expressed as data, not if/elif branches
# MAGIC
# MAGIC ## Data Sources
# MAGIC
# MAGIC Processing 11 vendor CSV files with varying levels of messiness:
# MAGIC - **Vendor A** (6 files): basic_clean, full_clean, messy_typos, messy_casing, messy_whitespace, excel_nightmare
# MAGIC - **Vendor B** (5 files): standard_clean, full_clean, messy_combo, excel_disaster, db_nightmare

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
import os
import glob
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

# Add workspace files to path for imports
workspace_files_path = "/Workspace" + os.path.dirname(os.getcwd())
if workspace_files_path not in sys.path:
    sys.path.insert(0, workspace_files_path)

# Import the CSV parser
from src.parse.base import CSVTableParser

# Configuration widgets
dbutils.widgets.text("catalog", "workspace", "Catalog Name")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema Name")
dbutils.widgets.text("silver_schema", "silver", "Silver Schema Name")
dbutils.widgets.text("incoming_volume", "incoming", "Incoming Volume Name")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
incoming_volume = dbutils.widgets.get("incoming_volume")

VOLUME_PATH = f"/Volumes/{catalog}/{bronze_schema}/{incoming_volume}"
print(f"📂 Using volume path: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper Functions

# COMMAND ----------


def get_csv_file_path(base_path: str) -> str:
    """
    Databricks writes CSVs as directories with part files inside.
    This finds the actual CSV file to read.

    Args:
        base_path: Base path to CSV directory

    Returns:
        Path to actual CSV file
    """
    csv_files = glob.glob(f"{base_path}/*.csv")
    if csv_files:
        return csv_files[0]
    else:
        raise FileNotFoundError(f"No CSV file found in {base_path}")


def extract_vendor_id(file_name: str) -> str:
    """
    Extract vendor ID from file name.

    Args:
        file_name: Name of the file (e.g., "vendor_a_basic_clean.csv")

    Returns:
        Vendor ID (e.g., "vendor_a")
    """
    if file_name.startswith("vendor_a"):
        return "vendor_a"
    elif file_name.startswith("vendor_b"):
        return "vendor_b"
    else:
        return "unknown"


print("✅ Helper functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bronze Layer: Unpivot All Vendor Files
# MAGIC
# MAGIC The unpivot transformation is where wide format becomes long format. Each cell in the original CSV becomes a row in the output. A 50-row CSV with 20 columns becomes 1,000 records (50 × 20).
# MAGIC
# MAGIC Position tracking (`row_index`, `column_index`) preserves the original structure. If an issue appears with a measurement, you can trace it back to the exact cell in the source file.
# MAGIC
# MAGIC The same loop processes all 11 vendor files. No vendor-specific branches. No special handling for typos. The parser treats every file identically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Define Files to Process

# COMMAND ----------

# Define all vendor files to process
vendor_files = [
    # Vendor A files
    "vendor_a_basic_clean.csv",
    "vendor_a_full_clean.csv",
    "vendor_a_basic_messy_typos.csv",
    "vendor_a_full_messy_casing.csv",
    "vendor_a_full_messy_whitespace.csv",
    "vendor_a_basic_excel_nightmare.csv",
    # Vendor B files
    "vendor_b_standard_clean.csv",
    "vendor_b_full_clean.csv",
    "vendor_b_standard_messy_combo.csv",
    "vendor_b_full_excel_disaster.csv",
    "vendor_b_standard_db_nightmare.csv",
]

print(f"📋 Processing {len(vendor_files)} vendor files:")
for file in vendor_files:
    print(f"   - {file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Parse and Unpivot Each File

# COMMAND ----------

# Initialize parser with configuration
parser = CSVTableParser({"header_detection_threshold": 5})

# Track ingestion timestamp
ingestion_timestamp = datetime.now()

# Collect all parsed records
all_records = []

print("🔄 Parsing vendor files...\n")

for file_name in vendor_files:
    try:
        # Get full path and find actual CSV file
        file_base_path = f"{VOLUME_PATH}/{file_name}"
        csv_file_path = get_csv_file_path(file_base_path)

        # Parse and unpivot
        records = parser.parse(csv_file_path)

        # Add metadata to each record
        vendor_id = extract_vendor_id(file_name)
        for record in records:
            record["vendor_id"] = vendor_id
            record["file_name"] = file_name
            record["ingestion_timestamp"] = ingestion_timestamp

        all_records.extend(records)

        print(f"✅ {file_name:45} → {len(records):6,} records")

    except Exception as e:
        print(f"❌ {file_name:45} → Error: {e}")

print(f"\n📊 Total records collected: {len(all_records):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Bronze Table

# COMMAND ----------

# Define schema for bronze table
bronze_schema_def = StructType(
    [
        StructField("row_index", IntegerType(), False),
        StructField("column_index", IntegerType(), False),
        StructField("lab_provided_attribute", StringType(), False),
        StructField("lab_provided_value", StringType(), True),
        StructField("vendor_id", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

# Create Spark DataFrame
spark_df_bronze = spark.createDataFrame(all_records, schema=bronze_schema_def)

# Write to bronze table
bronze_table_name = f"{catalog}.{bronze_schema}.lab_samples_unpivoted"
spark_df_bronze.write.format("delta").mode("overwrite").saveAsTable(bronze_table_name)

print(f"✅ Bronze table created: {bronze_table_name}")
print(f"📊 Row count: {spark_df_bronze.count():,}")
print(f"📋 Columns: {spark_df_bronze.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Bronze Table

# COMMAND ----------

print("🔍 Sample records from bronze layer:\n")
display(spark_df_bronze.limit(20))

# COMMAND ----------

# Show row counts by vendor and file
print("📊 Records by vendor and file:\n")
display(
    spark_df_bronze.groupBy("vendor_id", "file_name")
    .count()
    .orderBy("vendor_id", "file_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Silver Layer: Standardization Through Data
# MAGIC
# MAGIC Bronze preserves chaos; silver brings order. The key insight: standardization happens through data (mapping tables), not code (if/elif logic).
# MAGIC
# MAGIC The unpivoted bronze table contains `lab_provided_attribute` values like "ph", "acidity", "copper_ppm", "cu_total". The silver layer resolves these to canonical names through joins with mapping tables.
# MAGIC
# MAGIC **Configuration over code:** Vendor differences are expressed as rows in mapping tables, not if/elif branches. Adding a vendor means inserting rows; changing mappings means updating rows. Database operations, not deployments.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Mapping Tables
# MAGIC
# MAGIC Two tables drive the standardization:
# MAGIC - **vendor_column_mapping**: Maps vendor-specific column names to canonical column IDs
# MAGIC - **canonical_column_definitions**: Defines canonical columns with categories and data types
# MAGIC
# MAGIC This isn't a full dimensional model yet; that's the gold layer's job. Silver establishes canonical naming and basic categorization.

# COMMAND ----------

# Load vendor column mapping table
spark_mapping = spark.table(f"{catalog}.{bronze_schema}.vendor_column_mapping")

print(f"📋 Vendor Column Mapping table:")
print(f"   Rows: {spark_mapping.count():,}")
display(spark_mapping.limit(10))

# COMMAND ----------

# Load canonical column definitions table
spark_canonical = spark.table(f"{catalog}.{silver_schema}.canonical_column_definitions")

print(f"📋 Canonical Column Definitions table:")
print(f"   Rows: {spark_canonical.count():,}")
display(spark_canonical.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Join Bronze with Mapping and Canonical Tables
# MAGIC
# MAGIC The left join handles unmapped columns gracefully; anything not in the mapping table gets NULL for canonical information. You can filter for `canonical_column_id IS NOT NULL` to get recognized columns, or keep everything for complete lineage.

# COMMAND ----------

# Define table references
mapping_table = f"{catalog}.{bronze_schema}.vendor_column_mapping"
canonical_table = f"{catalog}.{silver_schema}.canonical_column_definitions"
silver_table_name = f"{catalog}.{silver_schema}.lab_samples_standardized"

# Create silver DataFrame using SQL
spark_df_silver = spark.sql(f"""
SELECT
    -- Original bronze columns for lineage
    b.row_index,
    b.column_index,
    b.lab_provided_attribute,
    b.lab_provided_value,
    b.vendor_id,
    b.file_name,
    b.ingestion_timestamp,
    -- Standardized column information
    c.canonical_column_id,
    c.canonical_column_name,
    c.column_category,
    c.data_type
FROM {bronze_table_name} b
LEFT JOIN {mapping_table} m
    ON b.lab_provided_attribute = m.vendor_column_name
    AND b.vendor_id = m.vendor_id
LEFT JOIN {canonical_table} c
    ON m.canonical_column_id = c.canonical_column_id
""")

# Write to silver table
spark_df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table_name)

print(f"✅ Silver table created: {silver_table_name}")
print(f"📊 Row count: {spark_df_silver.count():,}")
print(f"📋 Columns: {spark_df_silver.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Silver Table

# COMMAND ----------

print("🔍 Sample standardized records from silver layer:\n")
display(spark_df_silver.filter(F.col("canonical_column_name").isNotNull()).limit(20))

# COMMAND ----------

# Show mapped vs unmapped attributes
print("📊 Mapping success by vendor:\n")
display(
    spark_df_silver.groupBy("vendor_id")
    .agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(F.col("canonical_column_id").isNotNull(), 1).otherwise(0)).alias(
            "mapped_records"
        ),
        F.sum(F.when(F.col("canonical_column_id").isNull(), 1).otherwise(0)).alias(
            "unmapped_records"
        ),
    )
    .orderBy("vendor_id")
)

# COMMAND ----------

# Show unmapped attributes (columns not yet in mapping table)
print("🔍 Unmapped attributes (add to vendor_column_mapping as needed):\n")
display(
    spark_df_silver.filter(F.col("canonical_column_id").isNull())
    .select("vendor_id", "lab_provided_attribute")
    .distinct()
    .orderBy("vendor_id", "lab_provided_attribute")
)

# COMMAND ----------

# Show standardized columns by category
print("📊 Canonical columns by category:\n")
display(
    spark_df_silver.filter(F.col("canonical_column_name").isNotNull())
    .groupBy("canonical_column_name", "column_category")
    .agg(
        F.countDistinct("vendor_id").alias("vendor_count"),
        F.count("*").alias("record_count"),
    )
    .orderBy("column_category", "canonical_column_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary: The Zen of It
# MAGIC
# MAGIC By treating column names as data instead of schema, we eliminated brittleness without eliminating complexity. Vendor chaos still exists, but it's no longer a code problem. Column name variations become rows in mapping tables. Schema evolution becomes data updates, not deployments.
# MAGIC
# MAGIC This echoes Kimball's staging area principle: preserve source structure before imposing semantics. The unpivoted bronze IS that source structure, with vendor chaos encoded as data rather than fought through transformations.
# MAGIC
# MAGIC ## Tables Created
# MAGIC
# MAGIC ✅ **Bronze Layer**: `lab_samples_unpivoted`
# MAGIC - Fixed schema: `row_index`, `column_index`, `lab_provided_attribute`, `lab_provided_value`
# MAGIC - Vendor-agnostic; same structure for every vendor
# MAGIC - Position tracking for full lineage back to source cells
# MAGIC
# MAGIC ✅ **Silver Layer**: `lab_samples_standardized`
# MAGIC - Canonical column names via mapping table joins
# MAGIC - Cross-vendor comparable
# MAGIC - Ready for gold layer dimensional modeling
# MAGIC
# MAGIC ## The Paradoxes
# MAGIC
# MAGIC - By giving up control (accepting any schema), we gain control (one ingestion path)
# MAGIC - By preserving more of what vendors send (typos included), we achieve better standardization (explicit mapping)
# MAGIC - By doing less transformation in bronze, we enable cleaner layer separation
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC - **Gold Layer**: Build proper star schema dimensions from canonical column definitions
# MAGIC - **Data Quality**: Add validation rules and monitoring for unmapped attributes
# MAGIC - **Dashboards**: Cross-vendor comparisons, mapping coverage, schema drift tracking

# COMMAND ----------
