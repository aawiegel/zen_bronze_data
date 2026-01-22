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
# MAGIC ![medallion](./Medallion.png)
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
# MAGIC Configure the catalog and schema names for our three layers and import relevant modules.

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

from datetime import datetime
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Raw Ingestion with Metadata
# MAGIC
# MAGIC The bronze layer preserves raw data with **minimal transformation**. To demonstrate, first, let's load the sample data and explore it.

# COMMAND ----------

# Read vendor CSV (keeping everything as strings for now)
df = spark.read.option("header", "true").csv(file_path).select(["*", "_metadata"])

print(f"Rows read: {df.count()}")
print("\nSource schema:")
df.printSchema()
print("\nFirst five rows:")
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we try to avoid making too many assumptions about the data and just load it as is into the system. This does not mean we're done! Just that we're breaking down the total extraction, transformation, and loading process into discrete, modular steps.
# MAGIC
# MAGIC Note the `.select(["*", "_metadata"])` part in loading the data. Databricks will actually track some basic information about the data provenance for you that in other systems you would have to track more manually. However, we also have additional metadata about the ingestion process to track:
# MAGIC
# MAGIC - `ingestion_id`: Unique identifier for this file ingest
# MAGIC - `file_row_number`: Identify original position in file
# MAGIC - `ingested_at`: Timestamp of the ingestion
# MAGIC - `data_source`: Source of the data (in this case vendor A, but could be say SaaS database X)
# MAGIC
# MAGIC We could get more exhaustive than this (for example, including a pipeline code version hash from git), but we'll keep it simple for now.

# COMMAND ----------

# Add batch metadata (this is the ONLY transformation in bronze)
df_bronze = (
    df
    # Extract key fields from Databricks' _metadata
    .withColumn("source_file_path", F.col("_metadata.file_path"))
    .withColumn("source_file_name", F.col("_metadata.file_name"))
    .withColumn("file_modified_at", F.col("_metadata.file_modification_time"))
    
    # Add our business metadata
    .withColumn("ingestion_id", F.lit(str(uuid4())))
    .withColumn("data_source", F.lit("vendor_a"))
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("file_row_number", F.monotonically_increasing_id())
)

# COMMAND ----------

# Write to bronze Delta table
df_bronze.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{bronze_schema}.vendor_a_samples_raw"
)

print(f"✓ Written to {catalog}.{bronze_schema}.vendor_a_samples_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC We can now query the data in bronze and see both the raw values and the associated ingestion metadata.

# COMMAND ----------

spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.vendor_a_samples_raw LIMIT 5").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer Key Points
# MAGIC
# MAGIC Notice what we **did**:
# MAGIC - ✅ Loaded the data into a SQL-queryable table
# MAGIC - ✅ Added metadata for tracking which file this came from and when it was loaded
# MAGIC
# MAGIC Notice what we **didn't do**:
# MAGIC - ❌ Convert data types (everything is still strings)
# MAGIC - ❌ Rename columns (including the Databricks-provided `_metadata` column)
# MAGIC - ❌ Validate or clean values
# MAGIC - ❌ Apply business logic
# MAGIC
# MAGIC Bronze is about **preservation**, not transformation. We just add additional context so when someone's dashboard breaks downstream we can fully track the provenance of the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Cleaning and Standardization
# MAGIC
# MAGIC The silver layer applies **business logic**:
# MAGIC - Convert string columns to proper types (dates, doubles)
# MAGIC - Standardize column names for consistent querying
# MAGIC - Preserve lineage (ingestion_id, file_row_number)
# MAGIC
# MAGIC This is where the data becomes **queryable and reliable**.

# COMMAND ----------



# Read from bronze
df_bronze = spark.table(f"{catalog}.{bronze_schema}.vendor_a_samples_raw")

# Apply transformations
df_silver = (
    df_bronze
    .withColumnsRenamed(
        {
            "_metadata": "databricks_ingestion_metadata",
            "data_source": "vendor_name",
        }
    )
    # Parse dates
    .withColumn("date_received", F.to_date(F.col("date_received"), "yyyy-MM-dd"))
    .withColumn("date_processed", F.to_date(F.col("date_processed"), "yyyy-MM-dd"))
    # Modify types
    .withColumn("ph", F.col("ph").cast(DoubleType()))
    .withColumn("copper_ppm", F.col("copper_ppm").cast(DoubleType()))
    .withColumn("zinc_ppm", F.col("zinc_ppm").cast(DoubleType()))
    # Add processing timestamp
    .withColumn("silver_processed_at", F.current_timestamp())
)

print("Silver schema:")
df_silver.printSchema()

# COMMAND ----------

# Write to silver Delta table
df_silver.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{silver_schema}.vendor_a_samples_cleaned"
)

print(f"✓ Written to {catalog}.{silver_schema}.vendor_samples_cleaned")

# COMMAND ----------

spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.vendor_a_samples_cleaned LIMIT 5").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Key Points
# MAGIC
# MAGIC Now we have **typed, standardized data**:
# MAGIC - ✅ Dates are actual `DateType` (not strings)
# MAGIC - ✅ Measurements are `DoubleType` (not strings)
# MAGIC - ✅ Column names are clear and consistent
# MAGIC - ✅ Lineage preserved
# MAGIC - ✅ Additional metadata added related to silver
# MAGIC
# MAGIC We could also further clean, validate, or standardize the data here as needed.
# MAGIC
# MAGIC This is the layer that **analysts query** for data exploration and reporting.
# MAGIC
# MAGIC What we haven't done:
# MAGIC - ❌ Put the data into the exact format for report Y
# MAGIC - ❌ Aggregated anything

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Analytics-Ready Aggregations
# MAGIC
# MAGIC The gold layer delivers **pre-processed datasets** for specific use cases:
# MAGIC - Daily summaries by lab and date
# MAGIC - Statistical aggregations (mean, stddev, min, max)
# MAGIC - Optimized for dashboard queries
# MAGIC
# MAGIC Instead of forcing analysts to write the same aggregation queries repeatedly,
# MAGIC we materialize common patterns in gold.

# COMMAND ----------

# Read from silver
df_silver = spark.table(f"{catalog}.{silver_schema}.vendor_a_samples_cleaned")

# Create daily summary aggregations
df_gold = (
    df_silver
    .withColumn("month_start", F.date_trunc("month", F.col("date_received")))
    .groupBy("month_start", "vendor_name")
    .agg(
        F.count("sample_barcode").alias("sample_count"),
        F.avg("ph").alias("avg_ph"),
        F.stddev("ph").alias("stddev_ph"),
        F.min("ph").alias("min_ph"),
        F.max("ph").alias("max_ph"),
        F.avg("copper_ppm").alias("avg_copper_ppm"),
        F.avg("zinc_ppm").alias("avg_zinc_ppm")
    )
    .withColumn("gold_processed_at", F.current_timestamp())
)

print("Gold schema:")
df_gold.printSchema()

# COMMAND ----------

df_gold.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{gold_schema}.monthly_vendor_a_summary"
)

print(f"✓ Written to {catalog}.{gold_schema}.monthly_vendor_a_summary")

# COMMAND ----------

spark.sql(f"SELECT * FROM {catalog}.{gold_schema}.monthly_vendor_a_summary ORDER BY month_start").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Key Points
# MAGIC
# MAGIC The gold layer provides **fast access to common metrics**:
# MAGIC - ✅ Summaries (no need to scan all samples)
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
