# CSV Table Parser

Parse CSV files from Databricks volumes and unpivot them into long format.

## Overview

The `CSVTableParser` reads wide-format CSV files and transforms them into long format where each cell becomes a row containing:
- `row_index`: The row number (1-indexed)
- `column_index`: The column number (1-indexed)
- `lab_provided_attribute`: The column name
- `lab_provided_value`: The cell value

This is particularly useful for normalizing vendor CSV files where different vendors use different column structures.

## Usage

### Basic Example

```python
from parse import CSVTableParser

# Create parser instance
parser = CSVTableParser()

# Parse CSV file from Databricks volume
records = parser.parse("/Volumes/catalog/schema/volume/vendor_a.csv")

# Records is a list of dicts
print(records[0])
# {
#     "row_index": 1,
#     "column_index": 1,
#     "lab_provided_attribute": "sample_barcode",
#     "lab_provided_value": "PYB0123456"
# }
```

### With Configuration

```python
# Configure parser options
config = {
    "encoding": "utf-8",
    "header_detection_threshold": 5,  # Minimum non-null columns for header
    "csv_opts": {"delimiter": ","}    # Additional csv.reader options
}

parser = CSVTableParser(config)
records = parser.parse("/path/to/file.csv")
```

### Use with Databricks

The parser works seamlessly with Databricks volumes and can be used in notebooks or jobs:

```python
# In a Databricks notebook
from parse import CSVTableParser

# Parse file from volume
parser = CSVTableParser()
records = parser.parse("/Volumes/bronze/lab_data/raw/vendor_b_20231015.csv")

# Convert to Spark DataFrame for further processing
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(records)

# Save as Delta table
df.write.format("delta").mode("overwrite").saveAsTable("bronze.lab_data_unpivoted")
```

## Features

- **Header Detection**: Automatically finds the header row by detecting the first row with sufficient non-null values
- **Column Cleaning**: Removes empty columns and deduplicates column names
- **Position Tracking**: Maintains row and column indices for full traceability
- **Databricks Compatible**: Works with volume paths (`/Volumes/...`)

## Design Decisions

- Uses Python's built-in `csv` module for reliability and performance
- Returns list of dicts (not pandas DataFrame) for better Spark integration
- Works with file paths instead of file objects for Databricks compatibility
- Simple, focused API with minimal dependencies
