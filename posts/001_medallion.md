# Medallion Architecture 101: Building Data Pipelines That Don't Fall Apart

Medallion architecture appears everywhere in modern data engineering. Bronze, Silver, Gold. Raw data, refined data, analytics-ready data. Every Databricks tutorial mentions it. Every lakehouse pitch deck includes the diagram. Every "modern data stack" blog post treats it as gospel.

Here's the part most people skip: the core ideas trace back to Ralph Kimball's data warehouse methodology from the 1990s. Kimball advocated for staging areas that preserved raw data, integration zones for applying business logic, and dimensional models for analytics delivery. His framework included thirty-four subsystems covering everything from data extraction to audit dimension management. The medallion pattern distills these principles into something clearer and more actionable: three layers with distinct responsibilities.

This evolution represents genuine improvement, not just rebranding. Kimball's warehouse architecture was comprehensive but complex, designed for batch ETL in relational databases. Medallion architecture preserves the wisdom about data quality and layer separation while adapting to lakehouse capabilities like schema evolution and streaming ingestion. We kept what worked and simplified what didn't. Bronze gets you on the podium. Silver refines your performance. Gold takes the championship.

The pattern itself is straightforward. Bronze layers ingest raw data with minimal transformation, preserving source formats and tracking metadata about where data originated. Silver layers apply business logic, cleaning and standardizing data into queryable formats. Gold layers deliver analytics-ready datasets, pre-aggregated and optimized for dashboards and reports.

![Medallion Architecture](Medallion.png)

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

### Metadata Tracking Patterns

There are several approaches to tracking ingestion metadata, each with different trade-offs:

**Embedded metadata** adds provenance columns directly to the bronze table. Every row carries its source file path, ingestion timestamp, and vendor identifier. This approach is simple to implement and query since no joins are needed to trace a record back to its source. The trade-off is that file-level information repeats across every row. In practice, columnar storage formats like Parquet and Delta compress these repetitive string values efficiently, making the storage overhead minimal.

**Separate ingestion table** maintains a dedicated `ingestion_metadata` table with a surrogate key (like a `batch_id`). Bronze rows reference this key, keeping the data table compact. Need to analyze ingestion patterns or identify missing vendor deliveries? Query the lightweight metadata table without scanning data. Need complete lineage for specific records? Join the tables. This pattern scales better for production systems with high ingestion volumes and complex monitoring requirements.

**Data vault-style through table** uses a link or satellite table to associate ingestion batches with data records. This supports many-to-many relationships: a record appearing across multiple loads, or a single load touching multiple target tables. This is the most flexible pattern for complex lineage scenarios but adds architectural complexity.

For this demonstration, we use embedded metadata. It keeps the focus on medallion layer mechanics without introducing additional tables. Production systems with high data volumes or complex ingestion monitoring should consider the separate table or data vault patterns.

### Ingesting with Databricks' `_metadata`

Databricks exposes file-level metadata automatically through a special `_metadata` struct column. By including it in our select, we get the source file path, file name, and modification time without any manual tracking. We supplement this with business-level metadata: a unique ingestion ID, the data source identifier, a timestamp, and a row position for ordering.

```python
from datetime import datetime
from uuid import uuid4
from pyspark.sql import functions as F

# Read vendor CSV, including Databricks file metadata
df = spark.read.option("header", "true").csv(file_path).select(["*", "_metadata"])

# Add ingestion metadata (the ONLY transformation in bronze)
df_bronze = (
    df
    # Extract from Databricks' _metadata struct
    .withColumn("source_file_path", F.col("_metadata.file_path"))
    .withColumn("source_file_name", F.col("_metadata.file_name"))
    .withColumn("file_modified_at", F.col("_metadata.file_modification_time"))
    # Add business metadata
    .withColumn("ingestion_id", F.lit(str(uuid4())))
    .withColumn("data_source", F.lit("vendor_a"))
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("file_row_number", F.monotonically_increasing_id())
)

# Write to bronze
df_bronze.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{bronze_schema}.vendor_a_samples_raw"
)
```

The bronze table now contains both the raw data and its provenance trail:
```
sample_barcode | lab_id       | date_received | ... | source_file_path              | source_file_name           | file_modified_at     | ingestion_id                          | data_source | ingested_at          | file_row_number
PYB6134-404166 | PSL-73 72846 | 2024-02-02   | ... | /Volumes/.../incoming/ven...  | vendor_a_basic_clean.csv   | 2024-12-16 09:00:00 | a7f3c891-4e2b-4d1a-9c8f-1b5e6a7c8d9e | vendor_a    | 2024-12-16 10:23:45 | 0
PYB8638-328304 | PSL-77 74041 | 2024-10-11   | ... | /Volumes/.../incoming/ven...  | vendor_a_basic_clean.csv   | 2024-12-16 09:00:00 | a7f3c891-4e2b-4d1a-9c8f-1b5e6a7c8d9e | vendor_a    | 2024-12-16 10:23:45 | 1
PYB7141-642256 | PSL-82 22558 | 2024-08-28   | ... | /Volumes/.../incoming/ven...  | vendor_a_basic_clean.csv   | 2024-12-16 09:00:00 | a7f3c891-4e2b-4d1a-9c8f-1b5e6a7c8d9e | vendor_a    | 2024-12-16 10:23:45 | 2
```

Notice what we did and didn't do. We loaded the data into a SQL-queryable table and added metadata so any record can be traced back to its source file and ingestion event. We did not convert data types (everything remains strings), rename columns, validate values, or apply business logic. Bronze is about preservation, not transformation. When someone's dashboard breaks downstream, the metadata lets us walk backward through the entire pipeline to find where problems originated.

## Silver Layer: Cleaning and Standardization

The silver layer applies business logic and standardization. Here we convert data types, apply semantic renaming, and transform raw vendor formats into consistent structures that downstream users can query reliably. Silver is where we enforce data contracts and catch quality issues before they propagate to analytics.

Our vendor data needs several transformations. The date columns arrive as strings and need conversion to proper date types. The numeric measurements remain strings from CSV parsing and require casting to doubles. We also apply minor semantic renaming: `data_source` becomes `vendor_name` to better reflect its business meaning, and we rename the raw `_metadata` struct to `databricks_ingestion_metadata` for clarity. The original measurement column names (`ph`, `copper_ppm`, `zinc_ppm`) are already clear and descriptive, so we keep them as-is.
```python
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType

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
    # Cast measurement types
    .withColumn("ph", F.col("ph").cast(DoubleType()))
    .withColumn("copper_ppm", F.col("copper_ppm").cast(DoubleType()))
    .withColumn("zinc_ppm", F.col("zinc_ppm").cast(DoubleType()))
    # Add processing timestamp
    .withColumn("silver_processed_at", F.current_timestamp())
)

# Write to silver
df_silver.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{silver_schema}.vendor_a_samples_cleaned"
)
```

The silver table now contains properly typed columns with semantic naming:
```
sample_barcode | lab_id       | date_received | date_processed | ph   | copper_ppm | zinc_ppm | ... | vendor_name | ingestion_id                          | file_row_number | silver_processed_at
PYB6134-404166 | PSL-73 72846 | 2024-02-02   | 2024-02-08     | 6.74 | 10.7       | 5.23     | ... | vendor_a    | a7f3c891-4e2b-4d1a-9c8f-1b5e6a7c8d9e | 0               | 2024-12-16 10:24:15
PYB8638-328304 | PSL-77 74041 | 2024-10-11   | 2024-10-17     | 6.43 | 6.34       | 5.64     | ... | vendor_a    | a7f3c891-4e2b-4d1a-9c8f-1b5e6a7c8d9e | 1               | 2024-12-16 10:24:15
PYB7141-642256 | PSL-82 22558 | 2024-08-28   | 2024-09-03     | 5.58 | 3.64       | 39.8     | ... | vendor_a    | a7f3c891-4e2b-4d1a-9c8f-1b5e6a7c8d9e | 2               | 2024-12-16 10:24:15
```

Silver serves multiple purposes beyond type conversion. Production implementations often augment data through joins with reference tables, split datasets into separate dimension and fact tables following dimensional modeling patterns, apply complex business calculations, or enrich records with derived attributes. This simple example focuses on basic standardization to establish the pattern. Later posts will explore more sophisticated transformations as our architecture evolves to handle real-world complexity.

Notice we preserved the ingestion identifier and file row number. These lineage columns allow us to trace any silver record back to its bronze source and ultimately to the original vendor file. This becomes essential when data quality issues emerge or when business users question specific values. We can walk backward through the entire pipeline to find where problems originated.

The silver layer also serves as a natural place for data quality checks. We could add validation rules here: pH values should fall between 0 and 14, concentration values should be non-negative, processing dates should occur after received dates. For this simple example we're skipping validation, but production implementations would include these checks.

## Gold Layer: Analytics-Ready Aggregations

The gold layer delivers pre-aggregated datasets optimized for specific analytical use cases. Rather than forcing analysts to repeatedly write the same aggregation queries against silver, we materialize common patterns. This improves query performance and ensures consistent metric definitions across dashboards and reports.

For our soil sample data, we'll create a monthly summary table that aggregates measurements by vendor. This serves common analytical questions: how many samples did each vendor deliver per month, what were the average pH and concentration levels, did measurements vary significantly across time periods.
```python
from pyspark.sql import functions as F

# Read from silver
df_silver = spark.table(f"{catalog}.{silver_schema}.vendor_a_samples_cleaned")

# Create monthly summary aggregations
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

# Write to gold
df_gold.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{gold_schema}.monthly_vendor_a_summary"
)
```

The gold table provides quick access to monthly summary statistics:
```
month_start | vendor_name | sample_count | avg_ph | stddev_ph | min_ph | max_ph | avg_copper_ppm | avg_zinc_ppm | gold_processed_at
2024-02-01  | vendor_a    | 5            | 6.38   | 0.53      | 5.46   | 7.06   | 5.37           | 8.31         | 2024-12-16 10:25:30
2024-03-01  | vendor_a    | 4            | 6.57   | 0.57      | 5.71   | 7.54   | 3.76           | 13.5         | 2024-12-16 10:25:30
2024-08-01  | vendor_a    | 5            | 6.21   | 0.42      | 5.58   | 6.74   | 5.89           | 18.6         | 2024-12-16 10:25:30
```

Dashboards query this gold table instead of aggregating silver data repeatedly. Business intelligence tools connect directly to gold for their visualizations. Data scientists pull from gold for initial exploration before diving into silver for detailed analysis. Each layer serves its purpose: bronze preserves raw truth, silver provides clean queryable data, gold optimizes for consumption.

## The Foundation Is Set

We've implemented medallion architecture in its ideal form. Bronze preserves raw vendor data with minimal transformation, tracking file lineage through embedded metadata columns drawn from Databricks' `_metadata` struct and our own business-level provenance fields. Silver applies business logic and standardization, converting string columns to proper types and applying semantic naming. Gold delivers pre-aggregated monthly summaries optimized for analytical consumption. Each layer maintains clear boundaries and serves a distinct purpose.

This discipline pays dividends when requirements change. Need to recalculate silver with different business rules? Bronze remains untouched as your source of truth. Analytics team wants new gold aggregations? Silver provides clean, typed data ready for transformation. Vendor changes their file format? Only bronze ingestion logic needs adjustment while downstream layers continue functioning.

The architecture we built assumes cooperative vendors who send clean files with stable schemas. Our sample data arrived perfectly formatted with consistent column names, valid data types, and no surprises. This scenario establishes the pattern and demonstrates proper layer separation. Kimball's audit dimension tables inspired the separate ingestion table pattern we discussed earlier, and that remains the better choice for production systems with high ingestion volumes or complex monitoring needs. For our demo, embedding metadata directly in bronze keeps the focus on layer mechanics. Kimball's staging areas become our bronze layer. His integration zones become our silver transformations. His dimensional delivery layer maps to gold aggregations. The terminology evolved but the wisdom remained constant.

Reality rarely cooperates this nicely. To see why, let's peek at what Vendor B sends:

```python
# Peek at Vendor B's file
vendor_b_path = f"/Volumes/{catalog}/{bronze_schema}/{incoming_volume}/vendor_b_standard_clean.csv"
df_vendor_b = spark.read.option("header", "true").csv(vendor_b_path)
df_vendor_b.printSchema()
```

```
root
 |-- sample_barcode: string
 |-- lab_id: string
 |-- date_received: string
 |-- date_processed: string
 |-- acidity: string
 |-- cu_total: string
 |-- zn_total: string
```

Wait. `acidity` instead of `ph`? `cu_total` instead of `copper_ppm`? `zn_total` instead of `zinc_ppm`?

Same measurements. Different column names. How do we handle this without writing vendor-specific transformation code in our silver layer? Do we create separate tables for each vendor? Vendor-specific case statements? A config file with column mappings that grows longer with every new vendor?

The next post explores what happens when vendors send chaos instead of clean CSVs (typos in column headers, unstable schemas, Excel nightmares) and how our clean three-layer architecture begins accumulating complexity. We'll discover an elegant solution that handles schema variations without drowning in vendor-specific code.

## References and Further Reading

Kimball, Ralph, and Margy Ross. *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling*, 3rd ed. Wiley, 2013. 
- Chapter 2 provides an overview of Kimball's dimensional modeling techniques
- Chapter 8 covers audit dimensions and metadata tracking patterns

The Kimball Group. "Dimensional Data Warehousing Resources." https://www.kimballgroup.com/
- Archive of articles, design tips, and dimensional modeling techniques from the originators of the methodology

Databricks. "What is the medallion lakehouse architecture?" Databricks Documentation. https://docs.databricks.com/lakehouse/medallion.html
- Official documentation on medallion architecture patterns and best practices

The complete code examples and demo notebooks for this blog series are available at: https://github.com/aawiegel/zen_bronze_data