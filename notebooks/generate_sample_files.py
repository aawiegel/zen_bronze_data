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
# MAGIC - Header chaos (typos, casing, whitespace, unnamed columns)
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
from src.labforge import vendors, chaos

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
dbutils.widgets.text("incoming_volume", "incoming", "Incoming Volume Name")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
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
            "add_unnamed_columns": False,
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
            "add_unnamed_columns": False,
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
            "add_unnamed_columns": False,
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
            "add_unnamed_columns": True,
            "num_unnamed": 3,
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
            "add_unnamed_columns": False,
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
            "add_unnamed_columns": True,
            "num_unnamed": 5,
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
            "add_unnamed_columns": False,
        },
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate and Write Files

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
    spark_df = spark.createDataFrame(df)
    spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        output_path
    )

    print(f"  ✓ Written to {output_path}")
    print(f"    - Rows: {len(df)}")
    print(f"    - Columns: {len(df.columns)}")
    print(f"    - Chaos applied: {file_config.get('add_chaos', False)}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Files generated successfully! You should now have:
# MAGIC - **Clean files** showing legitimate vendor schema variations
# MAGIC - **Messy files** with header typos, casing issues, whitespace
# MAGIC - **Excel nightmares** with unnamed columns
# MAGIC - **Database nightmares** with invalid column name characters (#, %, -)
# MAGIC
# MAGIC These files are ready to be processed by your bronze → silver transformation logic!

# COMMAND ----------

# Display the files in the volume
display(dbutils.fs.ls(VOLUME_PATH))
