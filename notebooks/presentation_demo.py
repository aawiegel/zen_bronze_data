# Databricks notebook source
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
# MAGIC For those new to the pattern:
# MAGIC - **Bronze** = Raw data, as close to source as possible
# MAGIC - **Silver** = Cleaned, conformed, business logic applied
# MAGIC - **Gold** = Aggregated, ready for analytics
# MAGIC
# MAGIC Today we're focusing on **Bronze** - and questioning what "raw" really means.

# COMMAND ----------

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
dbutils.widgets.text("incoming_volume", "incoming", "Incoming Volume Name")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
incoming_volume = dbutils.widgets.get("incoming_volume")

VOLUME_PATH = f"/Volumes/{catalog}/{bronze_schema}/{incoming_volume}"
print(f"📂 Using volume path: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Act 1: The Simple Beginning 🌟
# MAGIC
# MAGIC "We have a clean vendor file. Let's just load it to bronze!"

# COMMAND ----------

# Load a CLEAN vendor file (no chaos!)
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
# MAGIC ## Problem 2: Whitespace in Headers
# MAGIC
# MAGIC "Wait, some files have spaces around column names?!"

# COMMAND ----------

# Load file with whitespace chaos
spark_df_whitespace = spark.read.option("header", "true").csv(
    f"{VOLUME_PATH}/vendor_a_full_messy_whitespace.csv"
)

print("😱 Look at these column names:")
for col in spark_df_whitespace.columns:
    print(f"  '{col}' (length: {len(col)})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Approach 3 (Add Whitespace Cleaning)
# MAGIC
# MAGIC "Okay, we'll trim the column names first..."

# COMMAND ----------

def clean_whitespace(df):
    """Strip whitespace from all column names"""
    return df.select([F.col(f"`{c}`").alias(c.strip()) for c in df.columns])

spark_df_whitespace_cleaned = clean_whitespace(spark_df_whitespace)

print("✅ Fixed! Trimmed all column names...")
print("📊 Cleaned columns:", spark_df_whitespace_cleaned.columns)

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
# MAGIC ## Problem 4: Inconsistent Casing
# MAGIC
# MAGIC "SOME columns ARE rAnDoM CaSe?!"

# COMMAND ----------

# Load file with casing chaos
spark_df_casing = spark.read.option("header", "true").csv(
    f"{VOLUME_PATH}/vendor_a_full_messy_casing.csv"
)

print("🤪 Look at this casing chaos:")
for col in spark_df_casing.columns[:8]:
    print(f"  {col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Approach 5 (Add Case Normalization)
# MAGIC
# MAGIC "We'll just lowercase everything..."

# COMMAND ----------

def normalize_casing(df):
    """Normalize all column names to lowercase with underscores"""
    return df.select([F.col(f"`{c}`").alias(c.lower()) for c in df.columns])

spark_df_casing_normalized = normalize_casing(spark_df_casing)

print("✅ Fixed! Everything lowercase...")
print("📊 Normalized columns:", spark_df_casing_normalized.columns[:5])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Excel Nightmares
# MAGIC
# MAGIC "This file has metadata rows at the top AND empty columns on the right!"

# COMMAND ----------

# Load the "excel nightmare" file (has both metadata rows and empty padding)
spark_df_excel_nightmare = spark.read.option("header", "true").csv(
    f"{VOLUME_PATH}/vendor_a_basic_excel_nightmare.csv"
)

print("🤦 Look at this mess:")
print(f"  Column count: {len(spark_df_excel_nightmare.columns)}")
print(f"  Columns: {spark_df_excel_nightmare.columns[:10]}")
display(spark_df_excel_nightmare.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Approach 6 (Add Header Detection & Empty Column Removal)
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
# MAGIC ## Problem 6: Invalid Database Characters
# MAGIC
# MAGIC "Someone put # and % in the column names?!"

# COMMAND ----------

# Load file with invalid DB characters
spark_df_db_nightmare = spark.read.option("header", "true").csv(
    f"{VOLUME_PATH}/vendor_b_standard_db_nightmare.csv"
)

print("😤 Look at these column names:")
for col in spark_df_db_nightmare.columns[:10]:
    print(f"  {col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Approach 7 (Add Character Sanitization)
# MAGIC
# MAGIC "We'll strip out invalid characters..."

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
# MAGIC
# MAGIC ```python
# MAGIC def load_vendor_to_bronze(file_path, vendor_name):
# MAGIC     """
# MAGIC     Our 'simple' bronze layer that just loads raw data...
# MAGIC     """
# MAGIC     # Step 1: Read CSV
# MAGIC     df = spark.read.option("header", "true").csv(file_path)
# MAGIC
# MAGIC     # Step 2: Detect and skip metadata rows
# MAGIC     df = detect_and_skip_metadata(df)
# MAGIC
# MAGIC     # Step 3: Clean whitespace from headers
# MAGIC     df = clean_whitespace(df)
# MAGIC
# MAGIC     # Step 4: Fix typos in headers
# MAGIC     df = fix_typos(df)
# MAGIC
# MAGIC     # Step 5: Normalize casing
# MAGIC     df = normalize_casing(df)
# MAGIC
# MAGIC     # Step 6: Remove empty columns
# MAGIC     df = remove_empty_columns(df)
# MAGIC
# MAGIC     # Step 7: Sanitize invalid DB characters
# MAGIC     df = sanitize_db_chars(df)
# MAGIC
# MAGIC     # Step 8: Apply vendor-specific column mapping
# MAGIC     if vendor_name == "vendor_b":
# MAGIC         df = standardize_vendor_b_columns(df)
# MAGIC     elif vendor_name == "vendor_c":
# MAGIC         df = standardize_vendor_c_columns(df)  # And more for each vendor!
# MAGIC     # ... and so on for each new vendor
# MAGIC
# MAGIC     # Step 9: Write to bronze
# MAGIC     df.write.format("delta").mode("append").saveAsTable("bronze.lab_samples")
# MAGIC
# MAGIC     return df
# MAGIC ```

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
# MAGIC Here's the code that handles all that chaos:

# COMMAND ----------

import inspect
from src.parse import CSVTableParser

# Display the parser source code
print("=" * 80)
print("CSVTableParser Implementation")
print("=" * 80)
print(inspect.getsource(CSVTableParser))

# COMMAND ----------

# MAGIC %md
# MAGIC **Key things to notice:**
# MAGIC - This uses the `csv` module to load data in an unopinionated way (compared to pandas or pyspark)
# MAGIC - `remove_header()`: Detects where real data starts (skips metadata rows)
# MAGIC - `clean_columns()`: Removes empty columns and deduplicates names
# MAGIC - `unpivot()`: Transforms wide → long format
# MAGIC - Preserves row/column position for traceability
# MAGIC - Keeps original column names (even with typos!)
# MAGIC
# MAGIC Now let's see it in action...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Unpivot (Melt) the Data
# MAGIC
# MAGIC Transform wide format → long format. Each measurement becomes its own row.
# MAGIC
# MAGIC We'll use the "excel nightmare" file - it has EVERYTHING wrong with it!

# COMMAND ----------

# Use our CSV parser to unpivot the messy file
parser = CSVTableParser({"header_detection_threshold": 5})
records = parser.parse(f"{VOLUME_PATH}/vendor_a_basic_excel_nightmare.csv/part-00000-tid-8961082112397051538-fc688257-b742-4979-814c-837dadb18d51-139-1-c000.csv")

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
# MAGIC ### Step 2: Load the Mapping Table
# MAGIC
# MAGIC Now we just need ONE mapping table that says "this vendor's column name = this standard analyte".

# COMMAND ----------

# Load the mapping table (created by our data generation script)
spark_mapping = spark.table(f"{catalog}.{bronze_schema}.vendor_analyte_mapping")

print("📋 The mapping table:")
display(spark_mapping.filter(F.col("vendor_id") == "vendor_a").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Join to Standardize
# MAGIC
# MAGIC One simple join gives us standardized measurements!

# COMMAND ----------

# Join unpivoted data with mapping
spark_df_standardized = spark_df_unpivoted.join(
    spark_mapping,
    (spark_df_unpivoted.lab_provided_attribute == spark_mapping.vendor_column_name),
    "left"
).select(
    "row_index",
    "column_index",
    "lab_provided_attribute",
    "lab_provided_value",
    F.col("analyte_name").alias("standardized_analyte"),
    F.col("analyte_id")
)

print("✨ Standardized measurements:")
display(spark_df_standardized.filter(F.col("standardized_analyte").isNotNull()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's See How It Handles a COMPLETELY Different Vendor!

# COMMAND ----------

# Parse Vendor B's messy file (totally different schema!)
records_vendor_b = parser.parse(f"{VOLUME_PATH}/vendor_b_full_excel_disaster.csv/part-00000-tid-7387106036713521623-54db8045-3310-4ad3-8b0c-6d36db9bb6e9-143-1-c000.csv")
spark_df_vendor_b_unpivoted = spark.createDataFrame(records_vendor_b)

# Same join logic works!
spark_df_vendor_b_standardized = spark_df_vendor_b_unpivoted.join(
    spark_mapping,
    (spark_df_vendor_b_unpivoted.lab_provided_attribute == spark_mapping.vendor_column_name),
    "left"
).select(
    "row_index",
    "column_index",
    "lab_provided_attribute",
    "lab_provided_value",
    F.col("analyte_name").alias("standardized_analyte"),
    F.col("analyte_id")
)

print("✨ Vendor B standardized with THE SAME CODE:")
display(spark_df_vendor_b_standardized.filter(F.col("standardized_analyte").isNotNull()).limit(10))

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
# MAGIC
# MAGIC ```python
# MAGIC def load_vendor_to_bronze(file_path):
# MAGIC     """
# MAGIC     Bronze layer: Unpivot and land
# MAGIC     Vendor-agnostic!
# MAGIC     """
# MAGIC     # Parse CSV and unpivot
# MAGIC     parser = CSVTableParser()
# MAGIC     records = parser.parse(file_path)
# MAGIC     df = spark.createDataFrame(records)
# MAGIC
# MAGIC     # Write to bronze
# MAGIC     df.write.format("delta").mode("append").saveAsTable(
# MAGIC         "bronze.lab_samples_unpivoted"
# MAGIC     )
# MAGIC
# MAGIC     return df
# MAGIC ```
# MAGIC
# MAGIC That's it. Same code for ALL vendors. 🎉

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
# MAGIC
# MAGIC ```python
# MAGIC def bronze_to_silver():
# MAGIC     """
# MAGIC     Silver layer: Join with mapping, apply business rules
# MAGIC     """
# MAGIC     bronze_df = spark.table("bronze.lab_samples_unpivoted")
# MAGIC     mapping_df = spark.table("silver.vendor_analyte_mapping")
# MAGIC
# MAGIC     # Standardize via join
# MAGIC     silver_df = bronze_df.join(
# MAGIC         mapping_df,
# MAGIC         bronze_df.lab_provided_attribute == mapping_df.vendor_column_name,
# MAGIC         "left"
# MAGIC     )
# MAGIC
# MAGIC     # Apply business rules (data type conversion, validation, etc.)
# MAGIC     silver_df = silver_df.withColumn(
# MAGIC         "numeric_value",
# MAGIC         F.when(
# MAGIC             F.col("analyte_data_type") == "numeric",
# MAGIC             F.col("lab_provided_value").cast("double")
# MAGIC         )
# MAGIC     )
# MAGIC
# MAGIC     return silver_df
# MAGIC ```
# MAGIC
# MAGIC Clean, testable, maintainable. ✨

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

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Questions? 🤔
# MAGIC
# MAGIC **Common Objections:**
# MAGIC
# MAGIC **Q: "Doesn't using csv instead hurt performance?"**
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


