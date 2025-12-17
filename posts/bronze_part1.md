# Medallion Architecture 101: Building Data Pipelines That Don't Fall Apart

Medallion architecture appears everywhere in modern data engineering. Bronze, Silver, Gold. Raw data, refined data, analytics-ready data. Every Databricks tutorial mentions it. Every lakehouse pitch deck includes the diagram. Every "modern data stack" blog post treats it as gospel.

Here's the part most people skip: the core ideas trace back to Ralph Kimball's data warehouse methodology from the 1990s. Kimball advocated for staging areas that preserved raw data, integration zones for applying business logic, and dimensional models for analytics delivery. His framework included thirty-four subsystems covering everything from data extraction to audit dimension management. The medallion pattern distills these principles into something clearer and more actionable: three layers with distinct responsibilities.

This evolution represents genuine improvement, not just rebranding. Kimball's warehouse architecture was comprehensive but complex, designed for batch ETL in relational databases. Medallion architecture preserves the wisdom about data quality and layer separation while adapting to lakehouse capabilities like schema evolution and streaming ingestion. We kept what worked and simplified what didn't. Bronze gets you on the podium. Silver refines your performance. Gold takes the championship.

The pattern itself is straightforward. Bronze layers ingest raw data with minimal transformation, preserving source formats and tracking metadata about where data originated. Silver layers apply business logic, cleaning and standardizing data into queryable formats. Gold layers deliver analytics-ready datasets, pre-aggregated and optimized for dashboards and reports.

This series starts with the ideal case. Clean vendor files, stable schemas, cooperative data sources. We'll implement proper medallion architecture with metadata tracking and layer discipline. Then reality arrives. Post two explores what happens when vendors send chaos instead of clean CSVs; typos in headers, unstable schemas, Excel nightmares that make you question your career choices. Post three reveals an elegant solution that handles the chaos without drowning in vendor-specific transformation code.

But first, the foundation. Let's build medallion architecture the right way.

---

**THE EVOLUTION**
```
Kimball's Data Warehouse (1996) → Medallion Lakehouse (2020)

34 subsystems          → 3 layers (Bronze, Silver, Gold)
Staging Area          → Bronze (raw ingestion, preserve source)
Integration Zone      → Silver (cleaned, standardized, queryable)  
Dimensional Delivery  → Gold (aggregated, analytics-ready)

Batch ETL             → Streaming + batch
Relational warehouses → Cloud lakehouses

Same core wisdom. Simpler execution.
```

---

## Understanding the Layers

Understanding medallion architecture requires understanding separation of concerns. Each layer serves one purpose. Bronze preserves raw data exactly as received, adding only metadata for tracking and auditability. Silver applies business rules and standardization, transforming raw inputs into consistent, queryable formats. Gold optimizes for specific analytical use cases, pre-aggregating and structuring data for dashboards, reports, and data science workflows.

The discipline matters more than the metaphor. Resist the temptation to "just quickly clean this in bronze" or "add one small aggregation to silver." Each compromise weakens the architecture. Bronze becomes unpredictable when transformations creep in. Silver becomes cluttered when analytics logic appears. Gold loses focus when it tries to serve every possible use case. Maintain clear boundaries between layers, and the entire pipeline becomes easier to debug, test, and extend.

Enough concepts. Let's implement this with actual code and actual data. We'll work with a clean vendor CSV file with stable schema.

## The Use Case

Biotech companies frequently work with contract research organizations to perform specialized laboratory measurements. These external labs analyze samples and return results as CSV or Excel files. Each vendor has their own file format, column naming conventions, and data delivery schedules. Our task is to ingest this vendor data and make it available for analysis while maintaining data quality and traceability.

Our scenario involves a soil chemistry lab that measures pH levels and heavy metal concentrations. They send results as CSV files with one row per sample. Each file contains sample identifiers, lab batch information, collection and processing dates, and measurement results. For this first post, we're working with the ideal case: clean data, stable schema, consistent formatting.

Here's what the vendor file looks like:
```csv
sample_barcode,lab_id,date_received,date_processed,ph,copper_ppm,zinc_ppm
PYB6134-404166,PSL-73 72846,2024-02-02,2024-02-08,6.74,10.7,5.23
PYB8638-328304,PSL-77 74041,2024-10-11,2024-10-17,6.43,6.34,5.64
PYB7141-642256,PSL-82 22558,2024-08-28,2024-09-03,5.58,3.64,39.8
```

Let's ingest this through our medallion architecture, tracking metadata at each stage and maintaining proper layer separation.

## Bronze Layer: Raw Ingestion with Metadata

The bronze layer preserves raw data with minimal transformation. The challenge lies in tracking provenance without corrupting the source data itself. Every record needs an audit trail: which file did this come from, when did it arrive, which vendor sent it. This metadata proves essential when data quality issues appear downstream or when business users question analytical results.

The naive approach duplicates file information across every row. Ingestion timestamp, file path, vendor identifier, and file hash get added as columns to the bronze table. This works but creates problems. Querying ingestion history requires scanning the entire bronze table even when you just want to know which files arrived last week. A file with ten thousand rows stores the same file path string ten thousand times, bloating storage unnecessarily. The metadata becomes inseparable from the data itself.

A cleaner pattern separates concerns: maintain an ingestion metadata table and reference it through a surrogate key. Need to analyze ingestion patterns, identify missing vendor deliveries, or audit file processing history? Query the lightweight metadata table. Need complete data lineage for specific records? Join the tables. This approach keeps bronze tables compact while making ingestion monitoring fast and efficient.

First, we register the file ingestion and generate a unique batch identifier:
```python
from datetime import datetime
from src.ingest.metadata import register_file_ingestion

# Register this file ingestion in metadata table
batch_metadata = register_file_ingestion(
    file_path="/Volumes/workspace/bronze/incoming/vendor_a_clean.csv",
    vendor_id="vendor_a",
    spark=spark
)

batch_id = batch_metadata["batch_id"]  # UUID for this ingestion
print(f"Registered batch: {batch_id}")
```

The metadata table tracks everything about the source file:
```
# workspace.bronze.ingestion_metadata table
batch_id                              | file_path                    | vendor_id | ingestion_timestamp  | file_size_bytes | file_hash
a7f3c891-4e2b-4d1a-9c8f-1b5e6a7c8d9e | /Volumes/.../vendor_a_cl... | vendor_a  | 2024-12-16 10:23:45 | 2048           | md5:a3b4c5...
```

Now we ingest the actual data, adding only the batch reference and row position:
```python
from pyspark.sql import functions as F

# Read vendor CSV
df = spark.read.option("header", "true").csv(
    "/Volumes/workspace/bronze/incoming/vendor_a_clean.csv"
)

# Add batch reference and row number
df_bronze = (
    df
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn("file_row_number", F.monotonically_increasing_id())
)

# Write to bronze
df_bronze.write.format("delta").mode("append").saveAsTable(
    "workspace.bronze.vendor_samples_raw"
)

display(df_bronze.limit(3))
```

The bronze table remains clean with just two metadata columns:
```
sample_barcode | lab_id       | date_received | date_processed | ph   | copper_ppm | zinc_ppm | batch_id                              | file_row_number
PYB6134-404166 | PSL-73 72846 | 2024-02-02   | 2024-02-08     | 6.74 | 10.7      | 5.23     | a7f3c891-4e2b-4d1a-9c8f-1b5e6a7c8d9e | 0
PYB8638-328304 | PSL-77 74041 | 2024-10-11   | 2024-10-17     | 6.43 | 6.34      | 5.64     | a7f3c891-4e2b-4d1a-9c8f-1b5e6a7c8d9e | 1
PYB7141-642256 | PSL-82 22558 | 2024-08-28   | 2024-09-03     | 5.58 | 3.64      | 39.8     | a7f3c891-4e2b-4d1a-9c8f-1b5e6a7c8d9e | 2
```

When we need complete lineage information, we join back to the metadata table. This pattern keeps bronze tables compact while maintaining full auditability. Notice what we still did not do: clean column names, convert data types, validate values, or apply business logic. Bronze remains as close to the source as possible.

## Silver Layer: Cleaning and Standardization

The silver layer applies business logic and standardization. Here we convert data types, clean values, and transform raw vendor formats into consistent structures that downstream users can query reliably. Silver is where we enforce data contracts and catch quality issues before they propagate to analytics.

Our vendor data needs several transformations. The date columns arrive as strings and need conversion to proper date types. The numeric measurements remain strings from CSV parsing and require casting to doubles. Column names contain inconsistencies that make querying awkward. We'll standardize these while preserving lineage back to bronze through the batch identifier.
```python
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType

# Read from bronze
df_bronze = spark.table("workspace.bronze.vendor_samples_raw")

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

# Write to silver
df_silver.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.silver.vendor_samples_cleaned"
)

display(df_silver.limit(3))
```

The silver table now contains properly typed, consistently named columns:
```
batch_id     | file_row_number | sample_id      | lab_id       | received_date | processed_date | ph_value | copper_concentration_ppm | zinc_concentration_ppm | silver_processed_at
a7f3c891-... | 0              | PYB6134-404166 | PSL-73 72846 | 2024-02-02   | 2024-02-08    | 6.74     | 10.7                    | 5.23                  | 2024-12-16 10:24:15
a7f3c891-... | 1              | PYB8638-328304 | PSL-77 74041 | 2024-10-11   | 2024-10-17    | 6.43     | 6.34                    | 5.64                  | 2024-12-16 10:24:15
a7f3c891-... | 2              | PYB7141-642256 | PSL-82 22558 | 2024-08-28   | 2024-09-03    | 5.58     | 3.64                    | 39.8                  | 2024-12-16 10:24:15
```

Silver serves multiple purposes beyond type conversion and column renaming. Production implementations often augment data through joins with reference tables, split datasets into separate dimension and fact tables following dimensional modeling patterns, apply complex business calculations, or enrich records with derived attributes. This simple example focuses on basic standardization to establish the pattern. Later posts will explore more sophisticated transformations as our architecture evolves to handle real-world complexity.

Notice we preserved the batch identifier and file row number. These lineage columns allow us to trace any silver record back to its bronze source and ultimately to the original vendor file. This becomes essential when data quality issues emerge or when business users question specific values. We can walk backward through the entire pipeline to find where problems originated.

The silver layer also serves as a natural place for data quality checks. We could add validation rules here: pH values should fall between 0 and 14, concentration values should be non-negative, processing dates should occur after received dates. For this simple example we're skipping validation, but production implementations would include these checks.

## Gold Layer: Analytics-Ready Aggregations

The gold layer delivers pre-aggregated datasets optimized for specific analytical use cases. Rather than forcing analysts to repeatedly write the same aggregation queries against silver, we materialize common patterns. This improves query performance and ensures consistent metric definitions across dashboards and reports.

For our soil sample data, we'll create a daily summary table that aggregates measurements by lab and date. This serves common analytical questions: how many samples did each lab process per day, what were the average pH and concentration levels, did measurements vary significantly within batches.
```python
from pyspark.sql import functions as F

# Read from silver
df_silver = spark.table("workspace.silver.vendor_samples_cleaned")

# Create daily aggregations
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

# Write to gold
df_gold.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.daily_sample_summary"
)

display(df_gold.limit(3))
```

The gold table provides quick access to summary statistics:
```
received_date | lab_id       | sample_count | avg_ph | stddev_ph | min_ph | max_ph | avg_copper_ppm | avg_zinc_ppm | gold_processed_at
2024-02-02   | PSL-73 72846 | 15          | 6.82   | 0.34      | 6.21   | 7.15   | 11.3          | 5.87         | 2024-12-16 10:25:30
2024-08-28   | PSL-82 22558 | 23          | 5.67   | 0.51      | 4.98   | 6.45   | 4.12          | 38.2         | 2024-12-16 10:25:30
2024-10-11   | PSL-77 74041 | 18          | 6.51   | 0.28      | 6.12   | 6.89   | 6.89          | 6.01         | 2024-12-16 10:25:30
```

Dashboards query this gold table instead of aggregating silver data repeatedly. Business intelligence tools connect directly to gold for their visualizations. Data scientists pull from gold for initial exploration before diving into silver for detailed analysis. Each layer serves its purpose: bronze preserves raw truth, silver provides clean queryable data, gold optimizes for consumption.

## The Foundation Is Set

We've implemented medallion architecture in its ideal form. Bronze preserves raw vendor data with minimal transformation, tracking file lineage through a separate metadata table. Silver applies business logic and standardization, converting string columns to proper types and normalizing naming conventions. Gold delivers pre-aggregated summaries optimized for analytical consumption. Each layer maintains clear boundaries and serves a distinct purpose.

This discipline pays dividends when requirements change. Need to recalculate silver with different business rules? Bronze remains untouched as your source of truth. Analytics team wants new gold aggregations? Silver provides clean, typed data ready for transformation. Vendor changes their file format? Only bronze ingestion logic needs adjustment while downstream layers continue functioning.

The architecture we built assumes cooperative vendors who send clean files with stable schemas. Our sample data arrived perfectly formatted with consistent column names, valid data types, and no surprises. This scenario establishes the pattern and demonstrates proper layer separation. The separate metadata table tracking file ingestion comes directly from Kimball's data warehouse patterns. His audit dimension tables served the same purpose: track provenance without polluting fact tables. Kimball's staging areas become our bronze layer. His integration zones become our silver transformations. His dimensional delivery layer maps to gold aggregations. The terminology evolved but the wisdom remained constant.

Reality rarely cooperates this nicely. Vendors send files with typos in column headers, metadata rows mixed with data, empty columns from Excel exports, and schemas that change based on which analysis package customers ordered. The next post explores what happens when these problems arrive and how our clean three-layer architecture begins accumulating complexity. We'll see bronze layers transform from simple ingestion into elaborate preprocessing pipelines, and we'll question whether we're still building medallion architecture or accidentally creating something else entirely.

## References and Further Reading

Kimball, Ralph, and Margy Ross. *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling*, 3rd ed. Wiley, 2013. 
- Chapter 2 provides an overview of Kimball's dimensional modeling techniques
- Chapter 8 covers audit dimensions and metadata tracking patterns

The Kimball Group. "Dimensional Data Warehousing Resources." https://www.kimballgroup.com/
- Archive of articles, design tips, and dimensional modeling techniques from the originators of the methodology

Databricks. "What is the medallion lakehouse architecture?" Databricks Documentation. https://docs.databricks.com/lakehouse/medallion.html
- Official documentation on medallion architecture patterns and best practices

The complete code examples and demo notebooks for this blog series are available at: https://github.com/aawiegel/zen_bronze_data