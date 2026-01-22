# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion Architecture 101: The Ideal Case
# MAGIC
# MAGIC This notebook demonstrates the **medallion architecture pattern** using clean vendor data.
# MAGIC
# MAGIC ## The Three Layers
# MAGIC
# MAGIC - **Bronze**: Raw ingestion with minimal transformation, preserving source format
# MAGIC - **Silver**: Cleaned and standardized data with proper types and column names
# MAGIC - **Gold**: Analytics-ready aggregations optimized for dashboards and reports
# MAGIC
# MAGIC ## Our Use Case
# MAGIC
# MAGIC A soil chemistry lab (Vendor A) sends CSV files with sample measurements:
# MAGIC - Sample identifiers and lab batch IDs
# MAGIC - Collection and processing dates
# MAGIC - pH and heavy metal concentration measurements
# MAGIC
# MAGIC This is the **ideal scenario**: clean data, stable schema, consistent formatting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Configure the catalog and schema names for our three layers.

# COMMAND ----------

# Get parameters from widgets (for deployment via Asset Bundles)
dbutils.widgets.text("catalog", "workspace", "Catalog Name")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema Name")
dbutils.widgets.text("silver_schema", "silver", "Silver Schema Name")
dbutils.widgets.text("gold_schema", "gold", "Gold Schema Name")
dbutils.widgets.text("incoming_volume", "incoming", "Incoming Volume Name")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
incoming_volume = dbutils.widgets.get("incoming_volume")

# Construct file path
file_path = f"/Volumes/{catalog}/{bronze_schema}/{incoming_volume}/vendor_a_basic_clean.csv"

print(f"Catalog: {catalog}")
print(f"Bronze Schema: {bronze_schema}")
print(f"Silver Schema: {silver_schema}")
print(f"Gold Schema: {gold_schema}")
print(f"File Path: {file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Raw Ingestion with Metadata
# MAGIC
# MAGIC The bronze layer preserves raw data with **minimal transformation**. We add only:
# MAGIC - `batch_id`: Unique identifier for this file ingestion
# MAGIC - `file_row_number`: Row position in the source file
# MAGIC
# MAGIC Everything else stays exactly as the vendor sent it.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
from uuid import uuid4

# Register this file ingestion
batch_id = str(uuid4())
ingestion_timestamp = datetime.now()

print(f"Batch ID: {batch_id}")
print(f"Ingestion Timestamp: {ingestion_timestamp}")

# COMMAND ----------

# Read vendor CSV (keeping everything as strings for now)
df = spark.read.option("header", "true").csv(file_path)

print(f"Rows read: {df.count()}")
print("\nSource schema:")
df.printSchema()

# COMMAND ----------

# Add batch metadata (this is the ONLY transformation in bronze)
df_bronze = (
    df
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn("file_row_number", F.monotonically_increasing_id())
)

# Write to bronze Delta table
df_bronze.write.format("delta").mode("append").saveAsTable(
    f"{catalog}.{bronze_schema}.vendor_samples_raw"
)

print(f"✓ Written to {catalog}.{bronze_schema}.vendor_samples_raw")

# COMMAND ----------

# Display bronze data
display(df_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer Key Points
# MAGIC
# MAGIC Notice what we **did**:
# MAGIC - ✅ Added `batch_id` for tracking which file this came from
# MAGIC - ✅ Added `file_row_number` for row-level lineage
# MAGIC
# MAGIC Notice what we **didn't do**:
# MAGIC - ❌ Convert data types (everything is still strings)
# MAGIC - ❌ Rename columns
# MAGIC - ❌ Validate or clean values
# MAGIC - ❌ Apply business logic
# MAGIC
# MAGIC Bronze is about **preservation**, not transformation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Cleaning and Standardization
# MAGIC
# MAGIC The silver layer applies **business logic**:
# MAGIC - Convert string columns to proper types (dates, doubles)
# MAGIC - Standardize column names for consistent querying
# MAGIC - Preserve lineage (batch_id, file_row_number)
# MAGIC
# MAGIC This is where the data becomes **queryable and reliable**.

# COMMAND ----------

from pyspark.sql.types import DoubleType, DateType

# Read from bronze
df_bronze = spark.table(f"{catalog}.{bronze_schema}.vendor_samples_raw")

# Apply transformations
df_silver = (
    df_bronze
    .select(
        # Preserve lineage
        F.col("batch_id"),
        F.col("file_row_number"),

        # Standardize column names and types
        F.col("sample_barcode").alias("sample_id"),
        F.col("lab_id"),
        F.to_date(F.col("date_received"), "yyyy-MM-dd").alias("received_date"),
        F.to_date(F.col("date_processed"), "yyyy-MM-dd").alias("processed_date"),
        F.col("ph").cast(DoubleType()).alias("ph_value"),
        F.col("copper_ppm").cast(DoubleType()).alias("copper_concentration_ppm"),
        F.col("zinc_ppm").cast(DoubleType()).alias("zinc_concentration_ppm")
    )
    # Add processing timestamp
    .withColumn("silver_processed_at", F.current_timestamp())
)

print("Silver schema:")
df_silver.printSchema()

# COMMAND ----------

# Write to silver Delta table
df_silver.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{silver_schema}.vendor_samples_cleaned"
)

print(f"✓ Written to {catalog}.{silver_schema}.vendor_samples_cleaned")

# COMMAND ----------

# Display silver data
display(df_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Key Points
# MAGIC
# MAGIC Now we have **typed, standardized data**:
# MAGIC - ✅ Dates are actual `DateType` (not strings)
# MAGIC - ✅ Measurements are `DoubleType` (not strings)
# MAGIC - ✅ Column names are clear and consistent
# MAGIC - ✅ Lineage preserved (`batch_id`, `file_row_number`)
# MAGIC
# MAGIC This is the layer that **analysts query** for data exploration and reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Analytics-Ready Aggregations
# MAGIC
# MAGIC The gold layer delivers **pre-aggregated datasets** for specific use cases:
# MAGIC - Daily summaries by lab and date
# MAGIC - Statistical aggregations (mean, stddev, min, max)
# MAGIC - Optimized for dashboard queries
# MAGIC
# MAGIC Instead of forcing analysts to write the same aggregation queries repeatedly,
# MAGIC we materialize common patterns in gold.

# COMMAND ----------

# Read from silver
df_silver = spark.table(f"{catalog}.{silver_schema}.vendor_samples_cleaned")

# Create daily summary aggregations
df_gold = (
    df_silver
    .groupBy("received_date", "lab_id")
    .agg(
        F.count("sample_id").alias("sample_count"),
        F.avg("ph_value").alias("avg_ph"),
        F.stddev("ph_value").alias("stddev_ph"),
        F.min("ph_value").alias("min_ph"),
        F.max("ph_value").alias("max_ph"),
        F.avg("copper_concentration_ppm").alias("avg_copper_ppm"),
        F.avg("zinc_concentration_ppm").alias("avg_zinc_ppm")
    )
    .withColumn("gold_processed_at", F.current_timestamp())
)

print("Gold schema:")
df_gold.printSchema()

# COMMAND ----------

# Write to gold Delta table
df_gold.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{gold_schema}.daily_sample_summary"
)

print(f"✓ Written to {catalog}.{gold_schema}.daily_sample_summary")

# COMMAND ----------

# Display gold data
display(df_gold.orderBy("received_date", "lab_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Key Points
# MAGIC
# MAGIC The gold layer provides **fast access to common metrics**:
# MAGIC - ✅ Daily summaries (no need to scan all samples)
# MAGIC - ✅ Statistical aggregations pre-calculated
# MAGIC - ✅ Optimized for dashboard queries
# MAGIC - ✅ Consistent metric definitions across reports

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: The Medallion Pattern Works!
# MAGIC
# MAGIC We successfully processed vendor data through three layers:
# MAGIC
# MAGIC | Layer | Purpose | Transformation |
# MAGIC |-------|---------|----------------|
# MAGIC | **Bronze** | Raw preservation | Minimal (only metadata) |
# MAGIC | **Silver** | Cleaned & standardized | Types, column names, business logic |
# MAGIC | **Gold** | Analytics-ready | Aggregations, pre-computed metrics |
# MAGIC
# MAGIC This architecture gives us:
# MAGIC - ✅ **Auditability**: Full lineage from gold → silver → bronze
# MAGIC - ✅ **Flexibility**: Can rebuild silver/gold if business logic changes
# MAGIC - ✅ **Performance**: Gold tables optimized for dashboards
# MAGIC - ✅ **Clarity**: Each layer has a clear, distinct purpose

# COMMAND ----------

# MAGIC %md
# MAGIC ## But Wait... What's This?
# MAGIC
# MAGIC That worked great for Vendor A. Clean data, stable schema, consistent formatting.
# MAGIC
# MAGIC Then **Vendor B** sends their file...

# COMMAND ----------

# Peek at Vendor B's file
vendor_b_path = f"/Volumes/{catalog}/{bronze_schema}/{incoming_volume}/vendor_b_standard_clean.csv"
df_vendor_b = spark.read.option("header", "true").csv(vendor_b_path)

print("Vendor B schema:")
df_vendor_b.printSchema()

print("\nVendor B sample data:")
display(df_vendor_b.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Plot Thickens 🤔
# MAGIC
# MAGIC Wait... what?
# MAGIC
# MAGIC - `acidity` instead of `ph`?
# MAGIC - `cu_total` instead of `copper_ppm`?
# MAGIC - `zn_total` instead of `zinc_ppm`?
# MAGIC
# MAGIC **Same measurements. Different column names.**
# MAGIC
# MAGIC How do we handle THIS without writing vendor-specific transformation code in our silver layer?
# MAGIC
# MAGIC Do we create separate tables for each vendor? Vendor-specific case statements?
# MAGIC A config file with column mappings that grows longer with every new vendor?
# MAGIC
# MAGIC ### Stay tuned for Part 2: "When Bronze Goes Rogue" 🔥
# MAGIC
# MAGIC We'll explore what happens when vendors send chaos instead of clean CSVs—and discover
# MAGIC an elegant solution that handles schema variations without drowning in vendor-specific code.
