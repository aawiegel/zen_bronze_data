# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze and Silver Layer Demo: Multi-Vendor Data Integration
# MAGIC
# MAGIC This notebook demonstrates loading vendor CSV files into bronze and silver layers using the unpivot approach.
# MAGIC
# MAGIC ## Process Overview
# MAGIC
# MAGIC 1. **Bronze Layer**: Parse and unpivot all vendor CSV files into long format
# MAGIC    - Handles messy headers, metadata rows, empty columns
# MAGIC    - Preserves original column names and values
# MAGIC    - Vendor-agnostic schema
# MAGIC
# MAGIC 2. **Silver Layer**: Join with mapping tables to standardize
# MAGIC    - Maps vendor-specific column names to canonical analytes
# MAGIC    - Adds analyte metadata (units, data types, valid ranges)
# MAGIC    - Ready for analysis and visualization
# MAGIC
# MAGIC ## Data Sources
# MAGIC
# MAGIC This notebook processes 11 vendor CSV files:
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

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
# MAGIC Parse all 11 vendor CSV files and combine them into a single long-format table.

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
# MAGIC # Silver Layer: Standardize with Mapping Tables
# MAGIC
# MAGIC Join bronze data with vendor analyte mapping and analyte dimension tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Mapping and Dimension Tables

# COMMAND ----------

# Load vendor analyte mapping table
spark_mapping = spark.table(f"{catalog}.{bronze_schema}.vendor_analyte_mapping")

print(f"📋 Vendor Analyte Mapping table:")
print(f"   Rows: {spark_mapping.count():,}")
display(spark_mapping.limit(10))

# COMMAND ----------

# Load analyte dimension table
spark_analyte_dim = spark.table(f"{catalog}.{silver_schema}.analyte_dimension")

print(f"📋 Analyte Dimension table:")
print(f"   Rows: {spark_analyte_dim.count():,}")
display(spark_analyte_dim.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Join Bronze with Mapping and Dimension Tables

# COMMAND ----------

# Read bronze table
spark_df_bronze_read = spark.table(bronze_table_name)

# Join with mapping table to get analyte IDs
spark_df_with_mapping = spark_df_bronze_read.join(
    spark_mapping,
    (spark_df_bronze_read.lab_provided_attribute == spark_mapping.vendor_column_name)
    & (spark_df_bronze_read.vendor_id == spark_mapping.vendor_id),
    "left",
)

# Join with analyte dimension to get analyte metadata
spark_df_silver = spark_df_with_mapping.join(
    spark_analyte_dim,
    spark_df_with_mapping.analyte_id == spark_analyte_dim.analyte_id,
    "left",
).select(
    # Bronze layer columns
    spark_df_bronze_read.row_index,
    spark_df_bronze_read.column_index,
    spark_df_bronze_read.lab_provided_attribute,
    spark_df_bronze_read.lab_provided_value,
    spark_df_bronze_read.vendor_id,
    spark_df_bronze_read.file_name,
    spark_df_bronze_read.ingestion_timestamp,
    # Mapped analyte information
    spark_analyte_dim.analyte_id,
    spark_analyte_dim.analyte_name,
    spark_analyte_dim.unit,
    spark_analyte_dim.data_type,
    spark_analyte_dim.min_valid_value,
    spark_analyte_dim.max_valid_value,
)

# Write to silver table
silver_table_name = f"{catalog}.{silver_schema}.lab_samples_standardized"
spark_df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table_name)

print(f"✅ Silver table created: {silver_table_name}")
print(f"📊 Row count: {spark_df_silver.count():,}")
print(f"📋 Columns: {spark_df_silver.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Silver Table

# COMMAND ----------

print("🔍 Sample standardized measurements from silver layer:\n")
display(spark_df_silver.filter(F.col("analyte_name").isNotNull()).limit(20))

# COMMAND ----------

# Show mapped vs unmapped attributes
print("📊 Mapping success by vendor:\n")
display(
    spark_df_silver.groupBy("vendor_id")
    .agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(F.col("analyte_id").isNotNull(), 1).otherwise(0)).alias(
            "mapped_records"
        ),
        F.sum(F.when(F.col("analyte_id").isNull(), 1).otherwise(0)).alias(
            "unmapped_records"
        ),
    )
    .orderBy("vendor_id")
)

# COMMAND ----------

# Show unmapped attributes (these are typically metadata columns like sample_barcode, lab_id, dates)
print("🔍 Unmapped attributes (typically metadata columns):\n")
display(
    spark_df_silver.filter(F.col("analyte_id").isNull())
    .select("vendor_id", "lab_provided_attribute")
    .distinct()
    .orderBy("vendor_id", "lab_provided_attribute")
)

# COMMAND ----------

# Show standardized analytes across vendors
print("📊 Analytes found across vendors:\n")
display(
    spark_df_silver.filter(F.col("analyte_name").isNotNull())
    .groupBy("analyte_name", "unit")
    .agg(
        F.countDistinct("vendor_id").alias("vendor_count"),
        F.count("*").alias("measurement_count"),
    )
    .orderBy("analyte_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC ## Tables Created
# MAGIC
# MAGIC ✅ **Bronze Layer**: `{bronze_table_name}`
# MAGIC - Unpivoted long format
# MAGIC - Preserves original column names and values
# MAGIC - Vendor-agnostic schema
# MAGIC - Tracks file metadata and ingestion timestamp
# MAGIC
# MAGIC ✅ **Silver Layer**: `{silver_table_name}`
# MAGIC - Standardized measurements with analyte metadata
# MAGIC - Cross-vendor comparable
# MAGIC - Ready for analysis and visualization
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Use the silver table to create dashboard visualizations showing:
# MAGIC - Cross-vendor analyte comparisons
# MAGIC - Data quality metrics (mapped vs unmapped attributes)
# MAGIC - Sample distributions by vendor and analyte type

# COMMAND ----------
