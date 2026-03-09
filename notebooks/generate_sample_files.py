# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Sample CSV Files for Bronze Layer
# MAGIC
# MAGIC This notebook generates synthetic vendor CSV files with realistic data quality issues
# MAGIC and writes them to the bronze incoming volume.
# MAGIC
# MAGIC **Demonstrates:**
# MAGIC - Multiple vendor schemas (Vendor A vs Vendor B)
# MAGIC - Additive measurement packages
# MAGIC - Header chaos (typos, casing, whitespace, invalid database characters)
# MAGIC - Metadata rows at the top of files (lab report headers, empty rows)
# MAGIC - Empty padding columns (blank column names with no data)
# MAGIC - Real-world CSV nightmares for demo purposes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
import os

# Add the workspace files directory to path so we can import src modules
# Databricks bundles sync files to /Workspace/Users/{user}/.bundle/{bundle_name}/{target}/files
workspace_files_path = "/Workspace" + os.path.dirname(os.getcwd())
if workspace_files_path not in sys.path:
    sys.path.insert(0, workspace_files_path)

import numpy as np
import pandas as pd
from src.labforge import vendors, chaos, metadata, customers, masking

# Initialize random generator for reproducibility
gen = np.random.default_rng(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Define the files we want to generate

# COMMAND ----------

# Get parameters from job (these are passed by the Databricks bundle)
# Default values are for local/manual runs
dbutils.widgets.text("catalog", "workspace", "Catalog Name")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema Name")
dbutils.widgets.text("silver_schema", "silver", "Silver Schema Name")
dbutils.widgets.text("incoming_volume", "incoming", "Incoming Volume Name")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
incoming_volume = dbutils.widgets.get("incoming_volume")

# Construct volume path
VOLUME_PATH = f"/Volumes/{catalog}/{bronze_schema}/{incoming_volume}"
print(f"Using volume path: {VOLUME_PATH}")

# File generation config
files_to_generate = [
    # Vendor A - Clean files
    {
        "filename": "vendor_a_basic_clean.csv",
        "vendor": "vendor_a",
        "packages": ["basic"],
        "rows": 50,
        "add_chaos": False,
    },
    {
        "filename": "vendor_a_full_clean.csv",
        "vendor": "vendor_a",
        "packages": ["basic", "metals", "micronutrient"],
        "rows": 75,
        "add_chaos": False,
    },
    # Vendor A - Messy files
    {
        "filename": "vendor_a_basic_messy_typos.csv",
        "vendor": "vendor_a",
        "packages": ["basic"],
        "rows": 50,
        "add_chaos": True,
        "chaos_config": {
            "header_typos": 0.8,
            "header_casing": 0.0,
            "header_whitespace": 0.0,
        },
    },
    {
        "filename": "vendor_a_full_messy_casing.csv",
        "vendor": "vendor_a",
        "packages": ["basic", "metals", "micronutrient"],
        "rows": 60,
        "add_chaos": True,
        "chaos_config": {
            "header_typos": 0.0,
            "header_casing": 0.9,
            "header_whitespace": 0.0,
        },
    },
    {
        "filename": "vendor_a_full_messy_whitespace.csv",
        "vendor": "vendor_a",
        "packages": ["basic", "metals"],
        "rows": 45,
        "add_chaos": True,
        "chaos_config": {
            "header_typos": 0.0,
            "header_casing": 0.0,
            "header_whitespace": 0.7,
        },
    },
    {
        "filename": "vendor_a_basic_excel_nightmare.csv",
        "vendor": "vendor_a",
        "packages": ["basic"],
        "rows": 40,
        "add_chaos": True,
        "chaos_config": {
            "header_typos": 0.3,
            "header_casing": 0.3,
            "header_whitespace": 0.2,
            "add_metadata_rows": True,
            "num_metadata": 2,
            "add_empty_padding": True,
            "num_empty": 3,
        },
    },
    {
        "filename": "vendor_a_basic_duplicate_barcodes.csv",
        "vendor": "vendor_a",
        "packages": ["basic"],
        "rows": 50,
        "add_chaos": True,
        "chaos_config": {
            "header_typos": 0.0,
            "header_casing": 0.0,
            "header_whitespace": 0.0,
            "num_duplicate_barcodes": 3,
        },
    },
    # Vendor B - Clean files
    {
        "filename": "vendor_b_standard_clean.csv",
        "vendor": "vendor_b",
        "packages": ["standard"],
        "rows": 50,
        "add_chaos": False,
    },
    {
        "filename": "vendor_b_full_clean.csv",
        "vendor": "vendor_b",
        "packages": ["standard", "heavy_metals", "trace_elements"],
        "rows": 80,
        "add_chaos": False,
    },
    # Vendor B - Messy files
    {
        "filename": "vendor_b_standard_messy_combo.csv",
        "vendor": "vendor_b",
        "packages": ["standard"],
        "rows": 55,
        "add_chaos": True,
        "chaos_config": {
            "header_typos": 0.5,
            "header_casing": 0.5,
            "header_whitespace": 0.3,
        },
    },
    {
        "filename": "vendor_b_full_excel_disaster.csv",
        "vendor": "vendor_b",
        "packages": ["standard", "heavy_metals", "trace_elements"],
        "rows": 70,
        "add_chaos": True,
        "chaos_config": {
            "header_typos": 0.6,
            "header_casing": 0.6,
            "header_whitespace": 0.4,
            "add_metadata_rows": True,
            "num_metadata": 3,
            "add_empty_padding": True,
            "num_empty": 5,
        },
    },
    {
        "filename": "vendor_b_standard_db_nightmare.csv",
        "vendor": "vendor_b",
        "packages": ["standard"],
        "rows": 50,
        "add_chaos": True,
        "chaos_config": {
            "header_typos": 0.0,
            "header_casing": 0.0,
            "header_whitespace": 0.0,
            "invalid_db_chars": 1.0,
        },
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Metadata Tables
# MAGIC
# MAGIC Two tables drive silver layer standardization:
# MAGIC - **canonical_column_definitions**: Defines canonical columns with categories and data types
# MAGIC - **vendor_column_mapping**: Maps vendor-specific column names to canonical IDs

# COMMAND ----------

print("Generating metadata tables...")

# Generate canonical column definitions table
canonical_path = f"{VOLUME_PATH}/canonical_column_definitions.csv"
metadata.generate_canonical_column_definitions_csv(canonical_path)
print(f"  ✓ Canonical column definitions written to {canonical_path}")

# Generate vendor column mapping table
mapping_path = f"{VOLUME_PATH}/vendor_column_mapping.csv"
metadata.generate_vendor_column_mapping_csv(mapping_path)
print(f"  ✓ Vendor column mapping written to {mapping_path}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate and Write Vendor Files

# COMMAND ----------

for file_config in files_to_generate:
    print(f"Generating {file_config['filename']}...")

    # Generate the vendor CSV
    df = vendors.forge_vendor_csv(
        gen,
        vendor=file_config["vendor"],
        packages=file_config["packages"],
        rows=file_config["rows"],
    )

    # Apply chaos if requested
    if file_config.get("add_chaos", False):
        chaos_config = file_config.get("chaos_config", {})
        df = chaos.apply_chaos(gen, df, **chaos_config)

    # Write to volume
    output_path = f"{VOLUME_PATH}/{file_config['filename']}"

    # Convert DataFrame to Spark DataFrame and write
    # If metadata rows were added, the DataFrame has integer column names
    # and we should write without header (header is now a data row)
    chaos_config = file_config.get("chaos_config", {})
    has_metadata = chaos_config.get("add_metadata_rows", False)
    header_option = "false" if has_metadata else "true"

    spark_df = spark.createDataFrame(df)
    spark_df.coalesce(1).write.mode("overwrite").option("header", header_option).csv(
        output_path
    )

    print(f"  ✓ Written to {output_path}")
    print(f"    - Rows: {len(df)}")
    print(f"    - Columns: {len(df.columns)}")
    print(f"    - Chaos applied: {file_config.get('add_chaos', False)}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customer and Sample Assignment Tables
# MAGIC
# MAGIC Creates two tables that model the many-to-one relationship between samples and customers:
# MAGIC - **customers.csv**: One row per unique customer (name, address, contact info, DOB)
# MAGIC - **customer_samples.csv**: One row per barcode, mapping it to a customer with submission-level fields (crop_type, sample_date)
# MAGIC
# MAGIC Barcodes are collected from the clean vendor files; the same generator seed means
# MAGIC re-running this notebook always produces identical output.

# COMMAND ----------

print("Generating customer and sample assignment tables...")

# Collect barcodes from the two clean vendor files
vendor_a_clean_df = (
    spark.read.option("header", "true")
    .csv(f"{VOLUME_PATH}/vendor_a_basic_clean.csv")
    .toPandas()
)
vendor_b_clean_df = (
    spark.read.option("header", "true")
    .csv(f"{VOLUME_PATH}/vendor_b_standard_clean.csv")
    .toPandas()
)

all_barcodes = list(
    pd.concat([vendor_a_clean_df["sample_barcode"], vendor_b_clean_df["sample_barcode"]]).unique()
)
print(f"  Collected {len(all_barcodes)} barcodes")

# Fewer customers than samples so multiple samples per customer is the norm
customer_df = customers.forge_customers(len(all_barcodes) // 5, gen)
customer_samples_df = customers.forge_customer_sample_assignments(all_barcodes, customer_df, gen)

# Write customers.csv
customers_path = f"{VOLUME_PATH}/customers.csv"
spark.createDataFrame(customer_df).coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(customers_path)
print(f"  ✓ Customers written to {customers_path} ({len(customer_df)} rows)")

# Write customer_samples.csv
customer_samples_path = f"{VOLUME_PATH}/customer_samples.csv"
spark.createDataFrame(customer_samples_df).coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(customer_samples_path)
print(f"  ✓ Customer samples written to {customer_samples_path} ({len(customer_samples_df)} rows)")
print()

# COMMAND ----------

print("Loading customer tables to Delta...")

customers_delta_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(customers_path)
)
customers_table_name = f"{catalog}.{bronze_schema}.customers"
customers_delta_df.write.mode("overwrite").saveAsTable(customers_table_name)
print(f"  ✓ Customers saved to {customers_table_name}")

customer_samples_delta_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(customer_samples_path)
)
customer_samples_table_name = f"{catalog}.{bronze_schema}.customer_samples"
customer_samples_delta_df.write.mode("overwrite").saveAsTable(customer_samples_table_name)
print(f"  ✓ Customer samples saved to {customer_samples_table_name}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Masked Customer Files for Comparison
# MAGIC
# MAGIC Applies five masking strategies to the synthetic customer data and writes
# MAGIC the masked versions alongside the originals in the volume.
# MAGIC
# MAGIC **Masking config applied:**
# MAGIC - `customer_id`: hash (same salt on both tables — join integrity preserved)
# MAGIC - `customer_name`: impute → "Anonymous"
# MAGIC - `date_of_birth`, `email`, `phone`, `street_address`, `city`: shuffle (values are real but belong to different customers)
# MAGIC - `age`: resample (distribution preserved, individual values replaced)
# MAGIC - `crop_type`: shuffle (commercially sensitive — who grows what is obscured)

# COMMAND ----------

MASK_SALT = "bronze-dev-2026"

customer_mask_config = {
    "customer_id": "hash",
    "customer_name": "impute",
    "date_of_birth": "shuffle",
    "email": "shuffle",
    "phone": "shuffle",
    "street_address": "shuffle",
    "city": "shuffle",
    "age": "resample",
}
customer_imputers = {"customer_name": lambda n: ["Anonymous"] * n}

masked_customer_df = masking.apply_masking(
    customer_df,
    customer_mask_config,
    generator=gen,
    imputers=customer_imputers,
    salt=MASK_SALT,
)

# customer_id must use the same salt so the FK join still works after masking
assignment_mask_config = {
    "customer_id": "hash",
    "crop_type": "shuffle",
}
masked_customer_samples_df = masking.apply_masking(
    customer_samples_df,
    assignment_mask_config,
    generator=gen,
    salt=MASK_SALT,
)

masked_customers_path = f"{VOLUME_PATH}/customers_masked.csv"
spark.createDataFrame(masked_customer_df).coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(masked_customers_path)
print(f"  ✓ Masked customers written to {masked_customers_path}")

masked_samples_path = f"{VOLUME_PATH}/customer_samples_masked.csv"
spark.createDataFrame(masked_customer_samples_df).coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(masked_samples_path)
print(f"  ✓ Masked customer samples written to {masked_samples_path}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Masked Customer Tables to Bronze


# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Metadata Tables to Delta

# COMMAND ----------

print("Loading metadata tables to Delta...")

# Load canonical column definitions to silver (reference data for standardization)
canonical_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(canonical_path)
)
canonical_table_name = f"{catalog}.{silver_schema}.canonical_column_definitions"
canonical_df.write.mode("overwrite").saveAsTable(canonical_table_name)
print(f"  ✓ Canonical column definitions saved to {canonical_table_name}")

# Load vendor column mapping to bronze (maps vendor columns to canonical IDs)
mapping_df = (
    spark.read.option("header", "true").option("inferSchema", "true").csv(mapping_path)
)
mapping_table_name = f"{catalog}.{bronze_schema}.vendor_column_mapping"
mapping_df.write.mode("overwrite").saveAsTable(mapping_table_name)
print(f"  ✓ Vendor column mapping saved to {mapping_table_name}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Files generated successfully! You should now have:
# MAGIC - **Mapping tables** (canonical_column_definitions.csv, vendor_column_mapping.csv)
# MAGIC - **Clean files** showing legitimate vendor schema variations
# MAGIC - **Messy files** with header typos, casing issues, whitespace
# MAGIC - **Excel nightmares** with metadata rows at the top and empty padding columns
# MAGIC - **Database nightmares** with invalid column name characters (#, %, -)
# MAGIC - **Duplicate barcode file** (vendor_a_basic_duplicate_barcodes.csv) — clean headers, 3 barcodes appear twice with different measurements (simulates technical replicates improperly assigned to existing barcodes)
# MAGIC - **Customer tables** (customers.csv, customer_samples.csv) — also loaded to bronze as `customers` and `customer_samples`
# MAGIC - **Masked customer files** (customers_masked.csv, customer_samples_masked.csv). Specifically not loaded to bronze
# MAGIC
# MAGIC These files are ready to be processed by your bronze → silver transformation logic!

# COMMAND ----------

# Display the files in the volume
display(dbutils.fs.ls(VOLUME_PATH))
