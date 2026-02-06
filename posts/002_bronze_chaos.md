# When Bronze Goes Rogue: Schema Chaos in the Wild

In [Part 1](./001_medallion.md), we explored the Medallion Architecture with clean, well-behaved vendor data. The bronze layer simply landed the raw CSV files. The silver layer standardized measurement names. The gold layer aggregated for analysis. Everything worked beautifully.

Then reality arrived.

This post demonstrates what happens when vendor CSV files exhibit the full spectrum of real-world data quality issues. We'll watch the bronze layer transform from "just land the data" into an increasingly complex series of transformations, vendor-specific logic, and fragile workarounds. By the end, we'll be asking uncomfortable questions about what "bronze" actually means.

The code examples are adapted from a [conference talk I gave at PyBay 2025](https://github.com/aawiegel/zen_bronze_data/blob/main/notebooks/pybay_presentation_2025-10.py), where I walked through this exact problem progression live.

## Problem 1: Different Column Names for the Same Measurements

Vendor A and Vendor B measure the same soil properties. Both provide pH, copper concentration, and zinc concentration. Their CSV files look like this:

**Vendor A schema:**
```
sample_barcode,lab_id,date_received,date_processed,ph,copper_ppm,zinc_ppm
```

**Vendor B schema:**
```
sample_barcode,lab_id,date_received,date_processed,acidity,cu_total,zn_total
```

Same measurements. Different names. Vendor B calls pH "acidity." They use chemical symbols with `_total` suffixes instead of element names with `_ppm` suffixes.

This is not a data quality problem. This is a legitimate difference in how two professional laboratories name their measurements. (Although pedantically you might wonder about a chemistry lab that thinks pH and acidity are the same thing.) Both schemas are internally consistent and well-documented. The challenge is ours: we need both vendors' data in the same bronze table.

### Bronze Layer: Approach 1 (Add Vendor-Specific Column Mapping)

We create a standardization function for each vendor:

```python
def standardize_vendor_a_columns(df):
    """Vendor A column names are already standard"""
    return df

def standardize_vendor_b_columns(df):
    """Map Vendor B columns to standard names"""
    return df.select(
        col("sample_barcode"),
        col("lab_id"),
        col("date_received"),
        col("date_processed"),
        col("acidity").alias("ph"),           # Different name
        col("cu_total").alias("copper_ppm"),  # Different name
        col("zn_total").alias("zinc_ppm"),    # Different name
    )
```

The bronze ingestion now includes a vendor detection step:

```python
def load_vendor_to_bronze(file_path, vendor_name):
    """Bronze layer with vendor-specific column mapping"""
    # Read CSV
    df = spark.read.option("header", "true").csv(file_path)

    # Apply vendor-specific standardization
    if vendor_name == "vendor_a":
        df = standardize_vendor_a_columns(df)
    elif vendor_name == "vendor_b":
        df = standardize_vendor_b_columns(df)

    # Write to bronze
    df.write.mode("append").saveAsTable("bronze.lab_samples")

    return df
```

This works. We can now query both vendors' data using consistent column names. The bronze layer contains standardized schemas.

But we just added vendor-specific business logic to what was supposed to be a raw data landing zone.

## Problem 2: Schema Instability Within the Same Vendor

The vendor-specific mapping holds up until Vendor A sends a new file. Our ingestion pipeline fails with a schema mismatch error. Examining the file reveals that Vendor A now includes additional analytes:

**Vendor A - Basic package (what we had):**
```
sample_barcode,lab_id,date_received,date_processed,ph,copper_ppm,zinc_ppm
```

**Vendor A - Metals package (what we just received):**
```
sample_barcode,lab_id,date_received,date_processed,ph,copper_ppm,zinc_ppm,lead_ppm,iron_ppm,manganese_ppm
```

The schema changes based on which analysis package the customer ordered. Sometimes they order basic soil testing. Sometimes they add heavy metals analysis. The vendor includes only the columns relevant to what was tested.

This is also not a data quality problem. Including only requested measurements is reasonable and reduces file size. But it breaks our bronze table schema.

### Bronze Layer: Approach 2 (Create Superset Schema)

The solution requires accommodating all possible variations. We create a superset schema containing all possible columns from all analysis packages. When ingesting files with fewer columns, we add NULL values for missing measurements:

```python
def align_to_superset_schema(df, vendor_name, analysis_package):
    """Add missing columns as NULL to match superset schema"""
    # Define superset of all possible columns for this vendor
    if vendor_name == "vendor_a":
        all_columns = [
            "sample_barcode", "lab_id", "date_received", "date_processed",
            "ph", "copper_ppm", "zinc_ppm",           # basic package
            "lead_ppm", "iron_ppm", "manganese_ppm",  # metals package
            # ... more columns as we discover them
        ]

    # Add missing columns as NULL
    for col in all_columns:
        if col not in df.columns:
            df = df.withColumn(col, lit(None))

    # Reorder columns to match superset
    df = df.select(all_columns)

    return df
```

Now our bronze ingestion tracks package types:

```python
def load_vendor_to_bronze(file_path, vendor_name, analysis_package):
    """Bronze layer with superset schema alignment"""
    df = spark.read.option("header", "true").csv(file_path)

    # Apply vendor-specific column mapping
    if vendor_name == "vendor_a":
        df = standardize_vendor_a_columns(df)
    elif vendor_name == "vendor_b":
        df = standardize_vendor_b_columns(df)

    # Align to superset schema
    df = align_to_superset_schema(df, vendor_name, analysis_package)

    # Write to bronze
    df.write.mode("append").option("mergeSchema", "true").saveAsTable("bronze.lab_samples")

    return df
```

This works. But now:
- Our bronze table is sparse (most columns are NULL for most rows)
- We must maintain a master list of all possible columns for each vendor
- Adding new analytes requires code changes
- We can't distinguish between "wasn't measured" and "measurement failed"

The bronze layer is accumulating knowledge about vendor schemas and business rules.

## Problem 3: Typos in Column Headers

Our superset schema handles varying column sets, but the next issue reveals a different category of problem. A file from Vendor A fails to parse correctly. Examining the raw CSV, we find:

```
sample_barcod,lab_id,date_recieved,date_proccessed,ph,copper_ppm,zinc_ppm
```

Three typos: `sample_barcod` (missing 'e'), `date_recieved` (i before e), `date_proccessed` (double c). The vendor's export system mangles column names. Occasionally.

These files are otherwise valid. The data values are correct. Only the header row has issues. Rejecting these files would delay processing by days while we contact the vendor.

### Bronze Layer: Approach 3 (Add Fuzzy Column Matching)

Rejecting files creates unacceptable delays, so we implement fuzzy matching to handle common typos:

```python
def fix_column_typos(df):
    """Fix common typos in column names"""
    column_mapping = {}

    for col in df.columns:
        col_lower = col.lower()

        # Check for common typos
        if "recieved" in col_lower:
            new_col = col.replace("recieved", "received").replace("reciev", "received")
        elif "proccessed" in col_lower:
            new_col = col.replace("proccessed", "processed").replace("proccess", "process")
        elif "barcod" in col_lower and "barcode" not in col_lower:
            new_col = col.replace("barcod", "barcode")
        else:
            new_col = col

        column_mapping[col] = new_col

    # Rename columns
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)

    return df
```

Our bronze ingestion grows:

```python
def load_vendor_to_bronze(file_path, vendor_name, analysis_package):
    """Bronze layer with fuzzy column matching"""
    df = spark.read.option("header", "true").csv(file_path)

    # Fix typos in column names
    df = fix_column_typos(df)

    # Apply vendor-specific column mapping
    if vendor_name == "vendor_a":
        df = standardize_vendor_a_columns(df)
    elif vendor_name == "vendor_b":
        df = standardize_vendor_b_columns(df)

    # Align to superset schema
    df = align_to_superset_schema(df, vendor_name, analysis_package)

    # Write to bronze
    df.write.mode("append").option("mergeSchema", "true").saveAsTable("bronze.lab_samples")

    return df
```

This works. But we're now making quality decisions about what constitutes an acceptable typo. We're interpreting intent. The bronze layer is no longer just landing raw data.

## Problem 4: Excel Nightmares

Vendor B sends a file that completely breaks our parser. Opening it in a text editor reveals the structure:

```
Contact:,lab@testing.com,"","","","","","","","","","","","","","","","",""
Generated:,2024-10-15,"","","","","","","","","","","","","","","","",""
Lab Name:,Premium Soil Testing,"","","","","","","","","","","","","","","","",""
Sampl_Barcode,lab_id,DATE_RECEIVED,Date_Proccessed,acidity,cu_totl,ZN_TOTL,pb_total,fe_total,Mn_Totl,b_total,mo_totl,ec_ms_cm,Organic_Carbon_Pct,"","","","",""
PYB2475-266277,AT6480 68463,2024-05-12,2024-05-19,6.46,6.63,29.5,4.22,103.,3.56,0.759,0.186,1.44,0.30,"","","","",""
```

Three metadata rows precede the actual header. Additionally, the file has empty column name padding (those trailing empty strings). The file exhibits the telltale signs of an Excel export where someone navigated beyond the data range and accidentally pressed enter before saving.

The actual data is fine. The measurements are valid. We just need to skip the metadata rows and ignore the empty columns.

### Bronze Layer: Approach 4 (Add Header Detection and Column Filtering)

We implement header detection to skip metadata:

```python
def detect_header_row(df):
    """Find the row that looks like a header (most non-null values)"""
    rows = df.limit(10).collect()

    header_idx = None
    max_non_null = 0

    for i, row in enumerate(rows):
        non_null_count = sum(1 for val in row if val is not None and val != "")
        if non_null_count > max_non_null:
            max_non_null = non_null_count
            header_idx = i

    return header_idx
```

We filter out empty columns:

```python
def remove_empty_columns(df):
    """Remove columns with empty names or all empty values"""
    cols_to_keep = []

    for col in df.columns:
        if col and col.strip():  # Has a name
            # Check if column has any non-empty values
            non_empty_count = df.filter(
                (col(col).isNotNull()) & (col(col) != "")
            ).count()

            if non_empty_count > 0:
                cols_to_keep.append(col)

    return df.select(cols_to_keep)
```

And implement re-reading from the correct header position:

```python
def reread_with_header_at_index(file_path, header_idx):
    """Re-read CSV file with header at a specific row index"""
    # Read the entire file as text, skip to header row
    df = (spark.read
          .option("header", "false")
          .csv(file_path))

    # Extract the header row
    header_row = df.collect()[header_idx]
    new_columns = [val for val in header_row if val]

    # Skip rows before header and use header_idx row as column names
    df = (spark.read
          .option("header", "false")
          .csv(file_path))

    # Filter to rows after header
    df = df.filter(monotonically_increasing_id() > header_idx)

    # Rename columns using detected header
    for i, col_name in enumerate(new_columns):
        df = df.withColumnRenamed(df.columns[i], col_name)

    return df
```

The bronze ingestion continues to grow:

```python
def load_vendor_to_bronze(file_path, vendor_name, analysis_package):
    """Bronze layer with header detection and column filtering"""
    # Read CSV without assuming first row is header
    df_peek = spark.read.csv(file_path)

    # Detect where the real header is
    header_idx = detect_header_row(df_peek)

    # Re-read with correct header
    df = reread_with_header_at_index(file_path, header_idx)

    # Remove empty columns
    df = remove_empty_columns(df)

    # Fix typos in column names
    df = fix_column_typos(df)

    # Apply vendor-specific column mapping
    if vendor_name == "vendor_a":
        df = standardize_vendor_a_columns(df)
    elif vendor_name == "vendor_b":
        df = standardize_vendor_b_columns(df)

    # Align to superset schema
    df = align_to_superset_schema(df, vendor_name, analysis_package)

    # Write to bronze
    df.write.mode("append").option("mergeSchema", "true").saveAsTable("bronze.lab_samples")

    return df
```

The bronze layer now includes heuristics for detecting valid data. We're making educated guesses about file structure.

## Problem 5: Database-Hostile Column Names

Vendor B's files sometimes include special characters in column names:

```
#sample_id,lab_id,organic_matter%,cu-total,zn-total
```

The `#` prefix, `%` suffix, and hyphens require backtick escaping in SQL queries:

```sql
SELECT `#sample_id`, `organic_matter%`, `cu-total`
FROM bronze.lab_samples
WHERE vendor = 'vendor_b'
```

Every analyst who touches this data must remember the escaping rules. Queries become brittle and harder to read.

### Bronze Layer: Approach 5 (Add Character Sanitization)

We sanitize column names to be database-friendly:

```python
def sanitize_column_names(df):
    """Remove invalid database characters from column names"""
    column_mapping = {}

    for col in df.columns:
        # Remove #, convert % to _pct, replace - with _
        new_col = col.replace("#", "").replace("%", "_pct").replace("-", "_")
        column_mapping[col] = new_col

    # Rename columns
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)

    return df
```

The complete bronze ingestion:

```python
def load_vendor_to_bronze(file_path, vendor_name, analysis_package):
    """Bronze layer: The final form"""
    # Read CSV to detect header
    df_peek = spark.read.csv(file_path)

    # Detect and skip metadata rows
    header_idx = detect_header_row(df_peek)

    # Re-read with correct header position
    df = reread_with_header_at_index(file_path, header_idx)

    # Remove empty columns
    df = remove_empty_columns(df)

    # Fix typos in column names
    df = fix_column_typos(df)

    # Sanitize database characters
    df = sanitize_column_names(df)

    # Apply vendor-specific column mapping
    if vendor_name == "vendor_a":
        df = standardize_vendor_a_columns(df)
    elif vendor_name == "vendor_b":
        df = standardize_vendor_b_columns(df)

    # Align to superset schema
    df = align_to_superset_schema(df, vendor_name, analysis_package)

    # Write to bronze
    df.write.mode("append").option("mergeSchema", "true").saveAsTable("bronze.lab_samples")

    return df
```

Eight transformation steps. Vendor-specific logic branches. Fuzzy matching heuristics. Schema knowledge. Quality decisions.

This was supposed to be "just land the raw data."

## The Uncomfortable Questions

We started with a simple bronze layer that read CSV files and wrote them to a table. We now have a complex ingestion pipeline that:

1. **Applies business logic:** Vendor-specific column mapping encodes knowledge about what measurements mean across different schemas
2. **Makes quality decisions:** Fuzzy matching determines which typos are acceptable and how to fix them
3. **Interprets structure:** Header detection guesses where real data begins
4. **Modifies data:** Character sanitization changes the raw column names we received

Is this still a "bronze layer"? The Medallion Architecture describes bronze as raw data with minimal transformation. We're well beyond minimal.

What happens when Vendor C arrives? We add more column mappings to the function, another branch in the if/elif chain, and hope their quirks don't conflict with the assumptions we've baked into our existing logic. And how do we decide what the "default" name for the column should be?

How do we test this? We need sample files for every vendor, every analysis package, every combination of issues. The test matrix grows exponentially.

Where does it end? 

Spoiler alert: It doesn't end. We haven't addressed date format differences, unit conversions, vendor-specific codes, or the dozens of other variations we'll encounter as more vendors join the system.

The bronze layer has gotten away from us.

## How Would You Manage This Complexity?

Before we explore solutions in the next post, consider how you would handle this problem in your own systems:

- Would you continue adding transformation logic to the bronze layer until it handles every edge case?
- Would you reject files that don't conform to expected formats and force vendors to fix their exports?
- Would you build a configuration system where new vendor quirks can be added without code changes?
- Would you accept some data quality issues and handle them downstream in the silver layer?

Each approach has tradeoffs. Adding more transformations makes the bronze layer complex and fragile. Rejecting files delays processing and frustrates vendors and users of the data alike. Configuration systems add their own complexity. Pushing problems downstream just moves the pain to a different layer.

What if the fundamental problem is that we're treating column names as schema when they should be treated as data?

In the next post, we'll explore this alternative. Instead of fighting schema chaos with increasingly complex transformations, we'll embrace it. We'll examine how a single transformation applied to all vendors can replace vendor-specific logic, superset schemas, fuzzy matching, and header detection with something simpler and more robust.

The solution involves questioning what "raw" actually means.

---

**Coming Soon:** Part 3 - The Zen of the Bronze Layer: Embracing Schema Chaos

**Code:** All examples are available in the [zen_bronze_data repository](https://github.com/aawiegel/zen_bronze_data). The [PyBay presentation notebook](https://github.com/aawiegel/zen_bronze_data/blob/main/notebooks/pybay_presentation_2025-10.py) contains runnable versions of these transformations.
