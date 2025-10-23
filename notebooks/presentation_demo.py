# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
import os
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Add workspace files to path for imports
workspace_files_path = "/Workspace" + os.path.dirname(os.getcwd())
if workspace_files_path not in sys.path:
    sys.path.insert(0, workspace_files_path)

# Configuration
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
# MAGIC # The Zen of the Bronze Layer
# MAGIC ## Ingestion of Data with Unstable Schema
# MAGIC
# MAGIC ### When "Keeping It Simple" Makes It Complex
# MAGIC
# MAGIC Today I want to show you how trying to keep our bronze ingestion layer "simple" made it incredibly **complex** -
# MAGIC and how going back to basics actually solved everything.
# MAGIC
# MAGIC **Background:** Often biotech companies work with contract research organizations to perform specialized measurements. Often, these lab vendors can only provide data back as CSV (or Excel) files. Even if they're performing the same types of measurements, each vendor has their own CSV schema. We want to ingest data from each vendor and make it available for analysis.
# MAGIC
# MAGIC **The Journey:**
# MAGIC 1. Start with a clean, simple file ✨
# MAGIC 2. Add complexity one problem at a time 😰
# MAGIC 3. Watch our bronze layer transform into spaghetti 🍝
# MAGIC 4. Discover a better way 💡

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Medallion Architecture
# MAGIC
# MAGIC ![](./Medallion.png)
# MAGIC
# MAGIC Today we're focusing on **Bronze** - and questioning what "raw" really means.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Act 1: The Simple Beginning 🌟
# MAGIC
# MAGIC "We have a clean vendor file. Let's just load it to bronze!"

# COMMAND ----------

spark_df_clean = spark.read.option("header", "true").csv(
    f"{VOLUME_PATH}/vendor_a_basic_clean.csv"
)

print("✅ Clean vendor file loaded!")
display(spark_df_clean.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Approach 1 (The Dream)
# MAGIC
# MAGIC Just save it! So simple! So elegant!

# COMMAND ----------

# Write to bronze
spark_df_clean.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{bronze_schema}.vendor_a_samples")

print("✅ Bronze table created!")
print("📊 Columns:", spark_df_clean.columns)
print("📈 Rows:", spark_df_clean.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### That was easy! What was the big deal? Can we just get a beer and call it a day?
# MAGIC
# MAGIC *...but then reality hits* 😅

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Act 2: Reality Hits 💀
# MAGIC
# MAGIC ## Problem 1: Different Column Names
# MAGIC
# MAGIC Vendor B uses different naming conventions for THE SAME measurements!

# COMMAND ----------

# Load Vendor B file (different schema)
spark_df_vendor_b = spark.read.option("header", "true").csv(
    f"{VOLUME_PATH}/vendor_b_standard_clean.csv"
)

print("🔍 Vendor A columns:", spark_df_clean.columns)
print("🔍 Vendor B columns:", spark_df_vendor_b.columns)
print("\n❌ They don't match! We need to standardize...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Approach 2 (Add Column Mapping)
# MAGIC
# MAGIC "No problem, we'll just map the columns!"

# COMMAND ----------

# Create a column mapping function
def standardize_vendor_b_columns(df):
    """Map Vendor B columns to standard names"""
    return df.select(
        F.col("sample_barcode"),
        F.col("lab_id"),
        F.col("date_received"),
        F.col("date_processed"),
        F.col("acidity").alias("ph"),  # Different name!
        F.col("cu_total").alias("copper_ppm"),  # Different name!
        F.col("zn_total").alias("zinc_ppm"),  # Different name!
    )

spark_df_vendor_b_standardized = standardize_vendor_b_columns(spark_df_vendor_b)

print("✅ Fixed! Now they match...")
print("📊 Standardized columns:", spark_df_vendor_b_standardized.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC So what's the big deal? And how about that beer?
# MAGIC
# MAGIC Well....
# MAGIC
# MAGIC ## Problem 2: Schema Instability
# MAGIC
# MAGIC "Wait... Vendor A sent a DIFFERENT set of columns this time?!"
# MAGIC
# MAGIC The vendor sends different columns depending on which analysis package the customer ordered.

# COMMAND ----------

# Load a Vendor A file with MORE columns (different analysis package)
spark_df_vendor_a_extended = spark.read.option("header", "true").csv(
    f"{VOLUME_PATH}/vendor_a_full_clean.csv"
)

print("🔍 Vendor A - Basic package:", spark_df_clean.columns)
print(f"   ({len(spark_df_clean.columns)} columns)")
print()
print("🔍 Vendor A - Full package:", spark_df_vendor_a_extended.columns)
print(f"   ({len(spark_df_vendor_a_extended.columns)} columns)")
print()
print("❌ The schema changes based on what analyses were ordered!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Approach 3 (Create Superset Schema)
# MAGIC
# MAGIC "We'll create a union schema with ALL possible columns..."

# COMMAND ----------

# Get the superset of all columns
all_columns = list(set(spark_df_clean.columns) | set(spark_df_vendor_a_extended.columns))
all_columns.sort()

print(f"📊 Superset schema has {len(all_columns)} columns:")
print(all_columns)
print()

# Add missing columns as NULL to each DataFrame
for col in all_columns:
    if col not in spark_df_clean.columns:
        spark_df_clean = spark_df_clean.withColumn(col, F.lit(None).cast(StringType()))
    if col not in spark_df_vendor_a_extended.columns:
        spark_df_vendor_a_extended = spark_df_vendor_a_extended.withColumn(col, F.lit(None).cast(StringType()))

# Reorder columns to match
spark_df_clean_aligned = spark_df_clean.select(all_columns)
spark_df_extended_aligned = spark_df_vendor_a_extended.select(all_columns)

# Now we can union them
spark_df_combined = spark_df_clean_aligned.union(spark_df_extended_aligned)

print("✅ Combined! But look at all those NULLs...")
display(spark_df_combined.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Problem Gets Worse
# MAGIC
# MAGIC "Now we have to track every possible column from every vendor AND every analysis package!"
# MAGIC
# MAGIC - ❌ Schema keeps growing as new analyses are added
# MAGIC - ❌ Most columns will be NULL for most rows (sparse table)
# MAGIC - ❌ Need to maintain a master list of all possible columns
# MAGIC - ❌ What happens when vendors add NEW analytes? Code changes!
# MAGIC - ❌ Can't tell which columns are "supposed" to be NULL vs missing data
# MAGIC
# MAGIC 😰 This approach doesn't scale...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: Typos in Headers
# MAGIC
# MAGIC "TYPOS?! They can't even spell 'received' consistently!"

# COMMAND ----------

# Load file with typos
spark_df_typos = spark.read.option("header", "true").csv(
    f"{VOLUME_PATH}/vendor_a_basic_messy_typos.csv"
)

print("😭 Look at these typos:")
for col in spark_df_typos.columns:
    print(f"  {col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Approach 4 (Add Fuzzy Matching)
# MAGIC
# MAGIC "Fine, we'll add fuzzy matching logic..."

# COMMAND ----------

def fix_typos(df):
    """Fix common typos in column names"""
    column_mapping = {}

    for col in df.columns:
        col_lower = col.lower()
        # Check for common typos
        if "recieved" in col_lower:  # received typo
            column_mapping[col] = col.replace("recieved", "received").replace("reciev", "received")
        elif "proccessed" in col_lower:  # processed typo
            column_mapping[col] = col.replace("proccessed", "processed").replace("proccess", "process")
        elif "sampl" in col_lower and "sample" not in col_lower:  # sample typo
            column_mapping[col] = col.replace("sampl", "sample")
        elif "barcod" in col_lower and "barcode" not in col_lower:  # barcode typo
            column_mapping[col] = col.replace("barcod", "barcode")
        else:
            column_mapping[col] = col

    return df.select([F.col(f"`{old}`").alias(new) for old, new in column_mapping.items()])

spark_df_typos_fixed = fix_typos(spark_df_typos)

print("✅ Fixed! (I think...)")
print("📊 Fixed columns:", spark_df_typos_fixed.columns[:5])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Excel Nightmares
# MAGIC
# MAGIC "This file has metadata rows at the top AND empty columns on the right!"

# COMMAND ----------

# MAGIC %md
# MAGIC ### First, let's look at the RAW file (before Spark tries to help)

# COMMAND ----------

# Find the actual CSV file (Spark writes as directory with part files)
import glob
csv_files = glob.glob(f"{VOLUME_PATH}/vendor_a_basic_excel_nightmare.csv/*.csv")
if csv_files:
    actual_file = csv_files[0]
    print("🔍 Raw CSV file (first 10 lines):")
    print("=" * 80)
    with open(actual_file, 'r') as f:
        for i, line in enumerate(f):
            if i >= 10:
                break
            print(f"{i+1:2d}: {repr(line.rstrip())}")
    print("=" * 80)
    print("\n😱 Notice:")
    print("  - Metadata rows at the top (Lab Report, Generated, etc.)")
    print("  - Empty column names (trailing commas)")
    print("  - First 'real' data row is around line 3-4")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now let's see what Spark does with it

# COMMAND ----------

# Load the "excel nightmare" file (has both metadata rows and empty padding)
spark_df_excel_nightmare = spark.read.option("header", "true").csv(
    f"{VOLUME_PATH}/vendor_a_basic_excel_nightmare.csv"
)

print("🤦 Spark 'helpfully' added column names:")
print(f"  Column count: {len(spark_df_excel_nightmare.columns)}")
print(f"  Columns: {spark_df_excel_nightmare.columns}")
print("\n  (Notice _c5, _c6, _c7 - Spark auto-named the empty columns)")
display(spark_df_excel_nightmare.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Approach 5 (Add Header Detection & Empty Column Removal)
# MAGIC
# MAGIC "We need to detect where the REAL header is AND remove empty columns..."
# MAGIC
# MAGIC *This is getting REALLY complicated now...*

# COMMAND ----------

def detect_and_skip_metadata(df):
    """
    Detect metadata rows and skip to actual data
    This is getting REALLY complicated now...
    """
    # Collect first few rows to inspect
    rows = df.limit(10).collect()

    # Find first row with mostly non-null values
    header_idx = None
    for i, row in enumerate(rows):
        non_null_count = sum(1 for val in row if val is not None and val != "")
        if non_null_count >= 5:  # Arbitrary threshold
            header_idx = i
            break

    print(f"⚠️ Found header at row {header_idx}")
    print("⚠️ Would need complex logic to skip rows and reset headers...")
    print("⚠️ This is getting really hacky...")
    return df

def remove_empty_columns(df):
    """Remove columns that are all null or empty string"""
    cols_to_keep = []

    for col in df.columns:
        if col and col.strip():  # Has a name
            # Check if column has any non-null, non-empty values
            non_empty_count = df.filter(
                (F.col(f"`{col}`").isNotNull()) & (F.col(f"`{col}`") != "")
            ).count()

            if non_empty_count > 0:
                cols_to_keep.append(col)

    return df.select([F.col(f"`{c}`") for c in cols_to_keep])

detect_and_skip_metadata(spark_df_excel_nightmare)
spark_df_excel_cleaned = remove_empty_columns(spark_df_excel_nightmare)

print(f"\n✅ Attempted fix...")
print(f"📊 Reduced column count: {len(spark_df_excel_cleaned.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Invalid or Annoying Database Characters
# MAGIC
# MAGIC "Someone put # and % in the column names?! Now I have to type `` each time I want to access a column!"

# COMMAND ----------

# Load file with invalid DB characters
spark_df_db_nightmare = spark.read.option("header", "true").csv(
    f"{VOLUME_PATH}/vendor_b_standard_db_nightmare.csv"
)

print("😤 Look at these column names:")
for col in spark_df_db_nightmare.columns:
    print(f"  {col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Approach 6 (Add Character Sanitization)
# MAGIC
# MAGIC "We'll strip out nonstandard characters..."

# COMMAND ----------

def sanitize_db_chars(df):
    """Remove invalid database characters from column names"""
    column_mapping = {}

    for col in df.columns:
        # Remove #, %, and replace - with _
        new_col = col.replace("#", "").replace("%", "_pct").replace("-", "_")
        column_mapping[col] = new_col

    return df.select([F.col(f"`{old}`").alias(new) for old, new in column_mapping.items()])

spark_df_db_sanitized = sanitize_db_chars(spark_df_db_nightmare)

print("✅ Fixed! Sanitized column names...")
print("📊 Sanitized columns:", spark_df_db_sanitized.columns[:8])

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Act 3: The Question 🤔
# MAGIC
# MAGIC ## Let's Look at Our "Bronze Layer" Pipeline Now...

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Bronze Layer Pipeline (Final Form)

# COMMAND ----------

# NOTE: This is for illustration only - not actually running this code
def load_vendor_to_bronze(file_path, vendor_name, analysis_package):
    """
    Our 'simple' bronze layer that just loads raw data...
    """
    # Step 1: Read CSV
    df = spark.read.option("header", "true").csv(file_path)

    # Step 2: Handle schema differences per analysis package
    df = align_to_superset_schema(df, vendor_name, analysis_package)

    # Step 3: Detect and skip metadata rows
    df = detect_and_skip_metadata(df)

    # Step 4: Fix typos in headers
    df = fix_typos(df)

    # Step 5: Remove empty columns
    df = remove_empty_columns(df)

    # Step 6: Sanitize invalid DB characters
    df = sanitize_db_chars(df)

    # Step 7: Apply vendor-specific column mapping
    if vendor_name == "vendor_b":
        df = standardize_vendor_b_columns(df)
    elif vendor_name == "vendor_c":
        df = standardize_vendor_c_columns(df)  # And more for each vendor!
    # ... and so on for each new vendor

    # Step 8: Write to bronze
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("bronze.lab_samples")

    return df

print("😰 Look at all those steps... and this is supposed to be 'just load the data'?!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨 The Uncomfortable Questions 🚨
# MAGIC
# MAGIC 1. **Is this still a "bronze layer"?**
# MAGIC    - We're doing 8+ transformations before landing the data
# MAGIC    - We're applying business logic (column standardization)
# MAGIC    - We're making decisions about data quality
# MAGIC
# MAGIC 2. **What happens when Vendor C arrives?**
# MAGIC    - Add ANOTHER column mapping function
# MAGIC    - Add ANOTHER branch to the if/elif chain
# MAGIC    - Hope their quirks don't break existing logic
# MAGIC
# MAGIC 3. **How do we test this?**
# MAGIC    - Need sample files for every vendor
# MAGIC    - Need to test every combination of issues
# MAGIC    - Integration tests become a nightmare
# MAGIC
# MAGIC 4. **Where does it end?**
# MAGIC    - What about date format differences?
# MAGIC    - What about unit conversions?
# MAGIC    - What about vendor-specific codes?

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Bronze Layer That Got Away From Us
# MAGIC
# MAGIC We started with "just load the raw data" and ended up with:
# MAGIC - 8 transformation steps
# MAGIC - Vendor-specific business logic
# MAGIC - Fragile string matching
# MAGIC - Complex conditional flows
# MAGIC - Difficult to test
# MAGIC - Scary to modify
# MAGIC
# MAGIC **This doesn't feel like "bronze" anymore... 😰**

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Act 4: The Pivot Revelation 💡
# MAGIC
# MAGIC ## What if we let ourselves be like leaves floating on the water and just accepted all this chaos?
# MAGIC
# MAGIC In other words, instead of trying to standardize the COLUMNS, what if we just started treating them like data?
# MAGIC
# MAGIC **The Insight:** The "raw" format isn't the wide CSV - it's the individual measurements!

# COMMAND ----------

# MAGIC %md
# MAGIC ### First, Let's Create a parser for this data
# MAGIC
# MAGIC Here's the code that handles all that chaos (simplified for presentation):

# COMMAND ----------

# CSVTableParser class (simplified version shown here for clarity)
# Full implementation at: src/parse/base.py

import csv
from typing import Any

class CSVTableParser:
    """Parse CSV files and unpivot to long format"""

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}

    def parse(self, file_path: str) -> list[dict[str, Any]]:
        """Main entry point: read CSV, clean, and unpivot"""
        # Step 1: Read the CSV file
        with open(file_path, "r", encoding=self.config.get("encoding", "utf-8")) as file:
            reader = csv.reader(file)
            records = [[val if val else None for val in row] for row in reader]

        # Step 2: Remove metadata rows and find the real header
        records = self.remove_header(
            records,
            min_found=self.config.get("header_detection_threshold", 10)
        )

        # Step 3: Clean up column names (remove empty columns, deduplicate)
        records = self.clean_columns(records)

        # Step 4: Unpivot from wide to long format
        records = self.unpivot(records)

        return records

    def remove_header(self, records: list, min_found: int = 10) -> list:
        """Detect where the real data starts by looking for mostly non-empty values"""
        for i, row in enumerate(records):
            non_empty = sum(1 for val in row if val is not None)
            if non_empty >= min_found:
                return records[i:]  # Start from this row
        return records

    def clean_columns(self, records: list) -> list:
        """Remove empty columns and deduplicate column names"""
        header = records[0]
        # Find non-empty columns and deduplicate names
        seen = {}
        indices_to_keep = []
        for i, col in enumerate(header):
            if col:  # Non-empty
                if col in seen:
                    seen[col] += 1
                    indices_to_keep.append((i, f"{col}_{seen[col]}"))
                else:
                    seen[col] = 0
                    indices_to_keep.append((i, col))

        # Rebuild records with only kept columns
        cleaned = []
        for row in records:
            cleaned.append([row[i] for i, _ in indices_to_keep])
        cleaned[0] = [name for _, name in indices_to_keep]  # Update header
        return cleaned

    def unpivot(self, records: list) -> list[dict[str, Any]]:
        """Transform wide format → long format"""
        header = records[0]
        result = []
        for row_idx, row in enumerate(records[1:], start=1):
            for col_idx, (attribute, value) in enumerate(zip(header, row), start=1):
                result.append({
                    "row_index": row_idx,
                    "column_index": col_idx,
                    "lab_provided_attribute": attribute,  # Keep original name!
                    "lab_provided_value": value,
                })
        return result

print("✨ Key features:")
print("  - Uses standard csv module (not pandas/spark)")
print("  - Detects real header row (skips metadata)")
print("  - Removes empty columns and deduplicates names")
print("  - Unpivots wide → long format")
print("  - Preserves original column names (typos and all!)")
print("  - Tracks row/column position for traceability")

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's see it in action...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Unpivot (Melt) the Data
# MAGIC
# MAGIC Transform wide format → long format. Each measurement becomes its own row.
# MAGIC
# MAGIC We'll use the "excel nightmare" file - it has EVERYTHING wrong with it!

# COMMAND ----------

import glob

# Helper function to find the actual CSV file in Databricks directory structure
def get_csv_file_path(base_path: str) -> str:
    """
    Databricks writes CSVs as directories with part files inside.
    This finds the actual CSV file to read.
    """
    csv_files = glob.glob(f"{base_path}/*.csv")
    if csv_files:
        return csv_files[0]
    else:
        raise FileNotFoundError(f"No CSV file found in {base_path}")

# Use our CSV parser to unpivot the messy file
parser = CSVTableParser({"header_detection_threshold": 5})
csv_file_path = get_csv_file_path(f"{VOLUME_PATH}/vendor_a_basic_excel_nightmare.csv")
records = parser.parse(csv_file_path)

# Convert to Spark DataFrame
spark_df_unpivoted = spark.createDataFrame(records)

print("✨ After unpivoting:")
display(spark_df_unpivoted.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Look What Just Happened! 🎉
# MAGIC
# MAGIC **Before (wide format):**
# MAGIC - Had to handle typos, casing, whitespace, metadata rows, empty columns
# MAGIC - Different schema per vendor
# MAGIC - Complex transformation logic
# MAGIC
# MAGIC **After (long format):**
# MAGIC - Every measurement is one row
# MAGIC - Same schema for ALL vendors
# MAGIC - Column names preserved as-is (even with typos!)
# MAGIC - Position tracking maintained
# MAGIC - Parser handled all the chaos for us!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Load the Mapping Tables
# MAGIC
# MAGIC We need TWO reference tables:
# MAGIC 1. **Vendor Mapping**: Maps vendor column names to standard analyte IDs
# MAGIC 2. **Analyte Dimension**: Contains metadata about each analyte (name, data type, units, etc.)

# COMMAND ----------

# Load the mapping table (vendor column names → analyte IDs)
spark_mapping = spark.table(f"{catalog}.{bronze_schema}.vendor_analyte_mapping")

print("📋 The vendor mapping table:")
display(spark_mapping.filter(F.col("vendor_id") == "vendor_a").limit(10))

# Load the analyte dimension table (analyte metadata)
spark_analyte_dim = spark.table(f"{catalog}.{silver_schema}.analyte_dimension")

print("\n📋 The analyte dimension table:")
display(spark_analyte_dim.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Join to Standardize
# MAGIC
# MAGIC Two simple joins give us standardized measurements with full metadata!

# COMMAND ----------

# Join unpivoted data with mapping, then with analyte dimension
spark_df_standardized = (
    spark_df_unpivoted
    .join(
        spark_mapping,
        (spark_df_unpivoted.lab_provided_attribute == spark_mapping.vendor_column_name),
        "left"
    )
    .join(
        spark_analyte_dim,
        spark_mapping.analyte_id == spark_analyte_dim.analyte_id,
        "left"
    )
    .select(
        "row_index",
        "column_index",
        "lab_provided_attribute",
        "lab_provided_value",
        spark_analyte_dim.analyte_id,
        spark_analyte_dim.analyte_name,
        spark_analyte_dim.data_type,
        spark_analyte_dim.unit
    )
)

print("✨ Standardized measurements with full metadata:")
display(spark_df_standardized.filter(F.col("analyte_name").isNotNull()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's See How It Handles a COMPLETELY Different Vendor!

# COMMAND ----------

# Parse Vendor B's messy file (totally different schema!)
csv_file_path_vendor_b = get_csv_file_path(f"{VOLUME_PATH}/vendor_b_full_excel_disaster.csv")
records_vendor_b = parser.parse(csv_file_path_vendor_b)
spark_df_vendor_b_unpivoted = spark.createDataFrame(records_vendor_b)

# Same join logic works!
spark_df_vendor_b_standardized = (
    spark_df_vendor_b_unpivoted
    .join(
        spark_mapping,
        (spark_df_vendor_b_unpivoted.lab_provided_attribute == spark_mapping.vendor_column_name),
        "left"
    )
    .join(
        spark_analyte_dim,
        spark_mapping.analyte_id == spark_analyte_dim.analyte_id,
        "left"
    )
    .select(
        "row_index",
        "column_index",
        "lab_provided_attribute",
        "lab_provided_value",
        spark_analyte_dim.analyte_id,
        spark_analyte_dim.analyte_name,
        spark_analyte_dim.data_type,
        spark_analyte_dim.unit
    )
)

print("✨ Vendor B standardized with THE SAME CODE:")
display(spark_df_vendor_b_standardized.filter(F.col("analyte_name").isNotNull()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Act 5: The Payoff 🎯
# MAGIC
# MAGIC ## Let's Compare the Two Approaches

# COMMAND ----------

# MAGIC %md
# MAGIC ### Approach 1: Traditional "Wide Format" Bronze
# MAGIC
# MAGIC **Complexity:**
# MAGIC - ❌ 8+ transformation functions
# MAGIC - ❌ Vendor-specific logic (if/elif chains)
# MAGIC - ❌ Fragile string matching for typos
# MAGIC - ❌ Hard-coded column mappings
# MAGIC - ❌ Difficult to test
# MAGIC
# MAGIC **Adding a New Vendor:**
# MAGIC - ❌ Write new column mapping function
# MAGIC - ❌ Add to if/elif chain
# MAGIC - ❌ Test all existing vendors still work
# MAGIC - ❌ Deploy code changes
# MAGIC
# MAGIC **Lines of Code:** ~200+ (and growing)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Approach 2: Unpivot + Mapping Table
# MAGIC
# MAGIC **Complexity:**
# MAGIC - ✅ 1 unpivot transformation
# MAGIC - ✅ 1 join with mapping table
# MAGIC - ✅ No vendor-specific code
# MAGIC - ✅ Handles typos/casing/whitespace automatically
# MAGIC - ✅ Easy to test
# MAGIC
# MAGIC **Adding a New Vendor:**
# MAGIC - ✅ Add rows to mapping table (DATA, not CODE)
# MAGIC - ✅ No code changes needed
# MAGIC - ✅ No deployment needed
# MAGIC - ✅ Can be done by non-developers
# MAGIC
# MAGIC **Lines of Code:** ~20

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Bronze Layer (Pivot Approach)

# COMMAND ----------

# NOTE: This is for illustration only - not actually running this code
def load_vendor_to_bronze(file_path):
    """
    Bronze layer: Unpivot and land
    Vendor-agnostic!
    """
    # Parse CSV and unpivot
    parser = CSVTableParser()
    records = parser.parse(file_path)
    df = spark.createDataFrame(records)

    # Write to bronze
    df.write.format("delta").mode("append").saveAsTable(
        "bronze.lab_samples_unpivoted"
    )

    return df

print("✨ That's it. Same code for ALL vendors. 🎉")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Breakthrough Insight 💡
# MAGIC
# MAGIC **Question:** "But isn't unpivoting a transformation? How is that 'raw'?"
# MAGIC
# MAGIC **Answer:** The unpivoted format is CLOSER to raw because:
# MAGIC 1. We're not applying business logic (standardization happens in Silver via mapping table)
# MAGIC 2. We preserve the exact column names (typos and all)
# MAGIC 3. We track position (row/column index) so we can reconstruct the original
# MAGIC 4. We're not making assumptions about what data means
# MAGIC
# MAGIC **The wide CSV is actually the "transformed" format** - it's how humans PRESENT data.
# MAGIC
# MAGIC The long format is the atomic unit - the raw measurement.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: Silver Layer Becomes Simpler Too

# COMMAND ----------

# NOTE: This is for illustration only - not actually running this code
def bronze_to_silver():
    """
    Silver layer: Join with mapping, apply business rules
    """
    bronze_df = spark.table("bronze.lab_samples_unpivoted")
    mapping_df = spark.table("silver.vendor_analyte_mapping")

    # Standardize via join
    silver_df = bronze_df.join(
        mapping_df,
        bronze_df.lab_provided_attribute == mapping_df.vendor_column_name,
        "left"
    )

    # Apply business rules (data type conversion, validation, etc.)
    silver_df = silver_df.withColumn(
        "numeric_value",
        F.when(
            F.col("analyte_data_type") == "numeric",
            F.col("lab_provided_value").cast("double")
        )
    )

    return silver_df

print("✨ Clean, testable, maintainable.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Important Caveats ⚠️
# MAGIC
# MAGIC ## This Isn't a Silver Bullet!
# MAGIC
# MAGIC Before you run off and pivot everything, let's talk about when this approach works and when it doesn't.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ When NOT to Use This Pattern
# MAGIC
# MAGIC **Entity-Attribute-Value (EAV) is usually an anti-pattern!**
# MAGIC
# MAGIC Don't use this approach if:
# MAGIC
# MAGIC 1. **You have a stable, well-defined schema**
# MAGIC    - If your data sources have consistent columns, just use a normal table!
# MAGIC    - EAV adds complexity you don't need
# MAGIC
# MAGIC 2. **You need to query the data in wide format frequently**
# MAGIC    - If analysts want `SELECT ph, copper_ppm, zinc_ppm FROM samples`, pivoting back is expensive
# MAGIC    - Only use EAV if the Bronze layer is truly just for landing raw data
# MAGIC    - Querying EAV tables can quickly turn nightmarish even for those well versed in SQL
# MAGIC
# MAGIC 3. **You have strong typing requirements**
# MAGIC    - Everything becomes a string in the `lab_provided_value` column
# MAGIC    - Type conversion happens in Silver, not Bronze
# MAGIC
# MAGIC 4. **Performance is critical**
# MAGIC    - EAV tables are generally slower to query than wide tables (depending on db)
# MAGIC    - More rows = more data to scan

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ When This Pattern Works Well
# MAGIC
# MAGIC This approach shines when:
# MAGIC
# MAGIC 1. **Schema is highly unstable**
# MAGIC    - Different vendors have different columns
# MAGIC    - Same vendor changes columns based on analysis package
# MAGIC    - New analytes or metadata are added regularly (This happens ALL the time in scientific research!!)
# MAGIC
# MAGIC 2. **You're dealing with semi-structured vendor data**
# MAGIC    - CSV/Excel files with vendor-specific quirks
# MAGIC    - Column names can't be trusted (typos, casing, whitespace)
# MAGIC    - Metadata rows and empty columns
# MAGIC
# MAGIC 3. **The Bronze layer is truly "just land it"**
# MAGIC    - Silver layer transforms to a proper wide format
# MAGIC    - Gold layer is where analysts query
# MAGIC    - Bronze is just the ingestion buffer
# MAGIC
# MAGIC 4. **Business logic is maintained by non-developers**
# MAGIC    - Column mappings can be updated via data, not code
# MAGIC    - No deployments needed for new vendors/analytes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔧 What We Left Out (For Simplicity)
# MAGIC
# MAGIC In a production implementation, you'd also need:
# MAGIC
# MAGIC 1. **File metadata tracking**
# MAGIC    - Which file did this data come from?
# MAGIC    - When was it ingested?
# MAGIC    - Original file path, size, checksum
# MAGIC
# MAGIC 2. **Row-level metadata**
# MAGIC    - Some columns aren't measurements (e.g., `sample_barcode`, `lab_id`, `date_received`)
# MAGIC    - These need to be pivoted back out or stored separately
# MAGIC    - Example: You'd want `sample_barcode` as a column in Silver, not an attribute
# MAGIC
# MAGIC 3. **Data quality flags**
# MAGIC    - Did the value parse successfully?
# MAGIC    - Was the column mapped to a known analyte?
# MAGIC    - Were there any warnings during parsing?
# MAGIC    - Data profiling / monitoring (e.g., we can see exactly how unstable the provided attributes are for each vendor!)    
# MAGIC
# MAGIC 4. **Incremental loading**
# MAGIC    - How do you avoid re-processing files?
# MAGIC    - Change data capture for mapping table updates

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary: What We Learned 🎓
# MAGIC
# MAGIC 1. **"Simple" bronze layers can become complex fast**
# MAGIC    - Each vendor quirk adds transformation logic
# MAGIC    - The code becomes vendor-specific and fragile
# MAGIC
# MAGIC 2. **Question your assumptions about "raw"**
# MAGIC    - Wide format CSVs are already a presentation layer
# MAGIC    - Long format is closer to the atomic data unit
# MAGIC
# MAGIC 3. **Pivot early, standardize with data not code**
# MAGIC    - Unpivot in bronze (same code for all vendors)
# MAGIC    - Use mapping tables for standardization (data changes, not code)
# MAGIC
# MAGIC 4. **Benefits of the pivot approach:**
# MAGIC    - ✅ Vendor-agnostic pipeline
# MAGIC    - ✅ Add new vendors without code changes
# MAGIC    - ✅ Easier testing
# MAGIC    - ✅ Simpler codebase
# MAGIC    - ✅ Non-developers can maintain mappings
# MAGIC
# MAGIC **Going back to basics made everything simpler.** 💡
# MAGIC
# MAGIC # Contact
# MAGIC
# MAGIC You can contact me at aawiegel@gmail.com
# MAGIC
# MAGIC https://www.linkedin.com/in/aawiegel/
# MAGIC
# MAGIC The source code is avaialble at https://github.com/aawiegel/zen_bronze_data
# MAGIC
# MAGIC ![](./gh_qr_code.png)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Questions? 🤔
# MAGIC
# MAGIC **Common Objections:**
# MAGIC
# MAGIC **Q: "Doesn't using the csv module instead hurt performance?"**
# MAGIC
# MAGIC A: The individual vendor files are typically not large. We're trading N vendor-specific transformation functions for 1 unpivot + 1 join. And Delta handles long-format data efficiently.
# MAGIC
# MAGIC **Q: "What about vendor-specific business logic?"**
# MAGIC
# MAGIC A: That belongs in Silver! Bronze should be agnostic. Silver is where you apply business rules to standardized data.
# MAGIC
# MAGIC **Q: "How do you handle missing columns?"**
# MAGIC
# MAGIC A: They just don't join to the mapping table. Easy to identify and handle in Silver.
# MAGIC
# MAGIC **Q: "What if vendors change their schema?"**
# MAGIC
# MAGIC A: Update the mapping table. No code changes needed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Thank you!** 🎉
# MAGIC
# MAGIC Questions? Comments? Horror stories about vendor CSV files?
# MAGIC
# MAGIC I'd love to hear them!

# COMMAND ----------


