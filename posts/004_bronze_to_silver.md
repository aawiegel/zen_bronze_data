# From Bronze to Silver: Staging, Intermediate, and the Art of the Trustworthy Join

In [Part 3](./003_zen_bronze.md), we solved vendor schema chaos by treating column names as data. The bronze layer now stores one row per attribute per measurement: `lab_provided_attribute` holds whatever the vendor typed as the column name, and `lab_provided_value` holds the cell value. The table schema is fixed and vendor-agnostic. No vendor-specific parsing logic, no superset schemas, no fuzzy matching. The chaos was encoded as rows rather than fought through transformations.

That was the right architecture for bronze ingestion. It is not the right format for answering analytical questions.

Ask "what was the average copper measurement for samples received in October?" against long-format EAV data and you're looking at a subquery to identify which `lab_provided_attribute` values represent copper, a pivot to bring those values into a column, a join to find the collection date from another EAV row, and a filter on the date. Do that once, and it's manageable. Do it in every query anyone ever writes against this data, and you've simply moved the transformation burden from ingestion to analysis. The point of a multi-layer pipeline is to make that transformation happen once, correctly, in a place where it can be tested and maintained.

Silver is where we make it happen. This post introduces the dbt models that carry bronze data from EAV chaos into an analytical schema, walks through the architectural reasoning behind each layer, and then runs the first unit test. It does not go well.

## Why dbt for Silver Transformations

Part 3 expressed the bronze layer in Python and PySpark. We could take the same approach for silver: write Spark SQL transformations directly, materialize staging and intermediate results as Delta tables, and wire them together with a notebook or a Databricks job. The transformation logic is the same regardless of the execution environment.

The reason to use dbt here is not that the SQL would be different, but that dbt enforces structural discipline that raw Spark SQL scripts don't provide by default.

Each dbt model lives in its own file with an explicit dependency graph. When `int_lab_samples_joined` references `ref('stg_lab_samples_unpivoted')`, dbt knows to run the staging model first, tracks the lineage between models, and rebuilds downstream models when upstream ones change. Schema documentation lives alongside the models in YAML files and can be generated into browsable documentation. Tests run against the models directly. Materialization strategies (view, table, incremental) are model-level configuration, not something you manage separately in SQL DDL. None of this is impossible with Spark SQL scripts, but it requires consistent discipline to maintain. dbt builds it into the workflow.

For a transformation layer with multiple models in a dependency chain, that structure is worth the overhead of learning a new tool. The silver layer here has at least three models with an explicit sequence: staging must run before intermediate, and getting that sequence wrong produces silent incorrect results rather than an obvious error. dbt makes those dependencies explicit and enforces them automatically.

## Why Silver Has Layers

Here is an important thing to know about dbt's `staging/` and `intermediate/` directory convention: Ralph Kimball described this architecture thirty years ago.

The Data Warehouse Toolkit (first published 1996, revised 2013) describes a staging area whose job is to clean and conform source data before it enters the warehouse. The staging area strips away source-system idiosyncrasies. It normalizes formats, resolves encoding variations, and creates consistent representations that downstream logic can depend on. The integration layer then applies business logic: joining conformed source data to reference tables, enriching records with canonical meaning, building the integrated representation that analysts actually query. dbt's best-practices guides on project structure \[1\] use exactly this division: staging models are thin, source-cleaning layers; intermediate models are where complex joins and business logic live.

The vocabulary has changed; the conceptual structure has not.

This matters because it explains why the layers exist, not just what they contain. The staging and intermediate separation encodes a functional dependency: intermediate models depend on guarantees that staging models make. Staging exists to create a contract. Intermediate relies on that contract to do its job. When those responsibilities blur, with source-system cleaning mixed into join logic and business rules embedded in normalization models, the result is transformation logic that is hard to reason about, hard to test, and hard to change safely.

With EAV bronze data, this layering matters more than usual. The staging layer has a specific, structural job to do before any downstream join is even possible.

## The Staging Model: Creating a Joinable Key

The problem with bronze EAV data is that `lab_provided_attribute` holds whatever the vendor typed. Vendor A sends `"Sample Concentration"`. Vendor B sends `"sample_concentration "` (trailing space included). A third vendor might send `"SAMPLE CONC."`. These represent the same measurement. No string comparison will match them without normalization.

The staging layer's job is to solve this problem in a way that downstream models can depend on. The model adds one column, `attribute_standardized`, that normalizes `lab_provided_attribute` into a consistent form:

```sql
{{ config(materialized='view') }}

WITH lab_samples_unpivoted AS (
    SELECT * FROM  {{ source('bronze', 'lab_samples_unpivoted') }}
),

lab_samples_unpivoted_staged AS (
    SELECT
        *,
        LOWER(
            TRIM(
                TRANSLATE(
                    lab_samples_unpivoted.lab_provided_attribute,
                    '-$()#./ %@!',
                    '___________'
                )
            )
        ) AS attribute_standardized
    FROM lab_samples_unpivoted
)

SELECT * FROM lab_samples_unpivoted_staged
```

The cleaning chain does three things. `TRANSLATE` replaces eleven special characters (`-$()#./ %@!`) with underscores, eliminating punctuation and symbols that would prevent string matching. `TRIM` strips leading and trailing whitespace. `LOWER` normalizes to lowercase so that capitalization differences don't produce false mismatches.

Applied to realistic vendor data: `"Sample Concentration"` becomes `sample_concentration`. `"SAMPLE CONC."` becomes `sample_conc_`. Something like `"-a$lot(of)weird#symbols.why/vendors%why@!"` becomes `a_lot_of_weird_symbols_why_vendors_why`.

Kimball called the underlying concept "conforming." Conformed dimensions bring disparate source systems into a shared vocabulary so they can be joined and analyzed together. You build a date dimension once, in a shared format, and every fact table that references dates joins to the same table. You don't maintain separate date representations for each source system. We call what the staging model does "attribute standardization." The goal is identical: create a canonical representation that downstream logic can join on without worrying about source-system variation. Same church, different pew.

Notice what this model does not do. It doesn't join to any reference data. It doesn't classify attributes or apply any business rules. It adds one column whose entire purpose is to enable the join that the next layer needs. That constraint is deliberate. dbt's best practices \[2\] describe staging models as thin layers that do the minimum necessary to make source data useful downstream: rename columns to consistent conventions, cast types, add computed identifiers. Business logic belongs in intermediate. The moment staging models start containing complex transformations, you've lost the separation that makes both layers maintainable.

## The Intermediate Layer: Enrichment Through Joining

With `attribute_standardized` available, the first intermediate model can do what staging made possible.

`int_lab_samples_joined` performs two joins. The first connects staged measurements to a vendor column mapping table on `attribute_standardized` and `vendor_id`. The mapping table is reference data that records the connection between vendor-specific attribute names and canonical column identifiers. The second join connects the mapping result to a canonical column definitions table, which carries authoritative metadata about each canonical column: its data type, its category (measurement, identifier, date), and whether it should be treated as a metadata column or a measurement column in downstream transformations.

```sql
{{ config(materialized='view') }}

WITH stg_lab_samples AS (
    SELECT * FROM {{ ref('stg_lab_samples_unpivoted') }}
),

vendor_column_mapping AS (
    SELECT * FROM {{ source('bronze', 'vendor_column_mapping') }}
),

canonical_column_definitions AS (
    SELECT * FROM {{ source('silver', 'canonical_column_definitions') }}
),

joined AS (
    SELECT
        * EXCEPT (
            vendor_column_mapping.vendor_id,
            vendor_column_mapping.vendor_column_name,
            vendor_column_mapping.canonical_column_id,
            vendor_column_mapping.notes
        ),
        notes AS vendor_mapping_notes
    FROM stg_lab_samples
    LEFT JOIN vendor_column_mapping
        ON stg_lab_samples.attribute_standardized = vendor_column_mapping.vendor_column_name
        AND stg_lab_samples.vendor_id = vendor_column_mapping.vendor_id
    LEFT JOIN canonical_column_definitions
        ON vendor_column_mapping.canonical_column_id = canonical_column_definitions.canonical_column_id
)

SELECT * FROM joined
```

The join condition includes both `attribute_standardized` and `vendor_id`. Two vendors might use similar standardized attribute names for genuinely different measurements, or the canonical mapping might differ by vendor for domain-specific reasons. Joining on both columns preserves that specificity.

The LEFT JOIN choice is a deliberate QA decision. Rows that don't match the mapping table, whether because of a vendor typo, a new analysis package, or a missing entry, don't disappear. They survive in the result set with `canonical_column_name IS NULL`. This is a signal, not missing data to be discarded. Downstream QA can filter on that null to find unmapped attributes and determine whether the mapping table needs a new entry, whether the vendor introduced something unexpected, or whether an existing mapping has a standardization error. The silver layer doesn't resolve that ambiguity; it surfaces it in a way that makes it findable.

This is Kimball's integration layer in practice \[3\]. A vendor-specific attribute in EAV format becomes a row that carries canonical column name, data type, category, and enrichment notes. The enrichment is what makes downstream transformations tractable. Without it, every downstream model or query would need to join to the mapping table itself, with full knowledge of vendor-specific attribute naming. That knowledge belongs here, applied once.

## The Intermediate Layer: Completing the Schema Transformation

The join model produces enriched EAV data: every row now knows its canonical column name and its category. The second intermediate model uses that enrichment to complete the transformation from EAV into something analysts can query directly.

The challenge is structural. In the bronze EAV format, metadata columns (sample barcode, lab ID, collection date, analysis date) live in the same row structure as measurement columns (copper, zinc, pH). A typical row from a sample with three measurements and four metadata columns produces seven EAV rows: four metadata rows and three measurement rows, all sharing the same `row_index`. Analysts want one record per measurement with the metadata attached as columns, not a set of EAV rows where metadata and measurements are interleaved.

`int_lab_samples_standardized` handles this with conditional aggregation:

```sql
{{ config(materialized='view') }}

WITH int_lab_samples_joined AS (
    SELECT * FROM {{ ref('int_lab_samples_joined') }}
),

metadata_pivoted AS (
    SELECT
        row_index,
        vendor_id,
        file_name,
        ingestion_timestamp,
        MAX(CASE WHEN canonical_column_name = 'sample_barcode' THEN lab_provided_value END) AS sample_barcode,
        MAX(CASE WHEN canonical_column_name = 'lab_id'         THEN lab_provided_value END) AS lab_id,
        MAX(CASE WHEN canonical_column_name = 'date_received'  THEN lab_provided_value END) AS date_received,
        MAX(CASE WHEN canonical_column_name = 'date_analyzed'  THEN lab_provided_value END) AS date_analyzed
    FROM int_lab_samples_joined
    WHERE is_metadata_column = TRUE
    GROUP BY row_index, vendor_id, file_name, ingestion_timestamp
),

-- All non-metadata rows, including unmapped ones (is_metadata_column IS NULL).
-- Unmapped rows are preserved for QA — filter on is_metadata_column IS NULL to find problem attributes.
measurements AS (
    SELECT * EXCEPT (is_metadata_column)
    FROM int_lab_samples_joined
    WHERE is_metadata_column = FALSE OR is_metadata_column IS NULL
),

standardized AS (
    SELECT
        measurements.* EXCEPT (file_name, ingestion_timestamp),
        metadata_pivoted.file_name,
        metadata_pivoted.ingestion_timestamp,
        metadata_pivoted.sample_barcode,
        metadata_pivoted.lab_id,
        metadata_pivoted.date_received,
        metadata_pivoted.date_analyzed
    FROM measurements
    LEFT JOIN metadata_pivoted
        ON measurements.row_index = metadata_pivoted.row_index
        AND measurements.vendor_id = metadata_pivoted.vendor_id
        AND measurements.file_name = metadata_pivoted.file_name
)

SELECT * FROM standardized
```

The logic splits the enriched EAV data into two paths. The `metadata_pivoted` CTE filters for rows where `is_metadata_column = TRUE` and collapses them into one record per `row_index`, `vendor_id`, and `file_name`, using `MAX(CASE WHEN ...)` to pull each metadata attribute into its own named column. The `measurements` CTE takes the other path: rows where `is_metadata_column = FALSE` (actual measurement attributes) and rows where `is_metadata_column IS NULL` (attributes that didn't match the mapping table) both pass through. The final join recombines the two paths on all three columns.

The three-column join condition deserves a note. `row_index` is the position of a row within a single CSV file; it resets to zero at the start of each file. Joining on `row_index` and `vendor_id` alone would incorrectly match row 5 from `file_a.csv` with row 5 from `file_b.csv` if both came from the same vendor. Including `file_name` makes the join key specific to a row within a specific file from a specific vendor: the granularity we actually want.

Two other design decisions in this model are worth making explicit.

The `is_metadata_column IS NULL` inclusion in the measurements CTE is the same philosophy as the LEFT JOIN in the previous model: preserve ambiguity rather than discard it. An unmapped row in silver is information. It says "something arrived that we haven't classified yet." Discarding it would make the gap invisible; surfacing it makes the gap findable.

Date columns (`date_received`, `date_analyzed`) remain as raw strings through the silver layer. Vendor date formats vary: one vendor might send `2024-01-15`, another might send `01/15/2024`. Casting to a date type without knowing the vendor's format either fails on unexpected formats or silently coerces values incorrectly. The gold layer, which has full vendor context, is where that conversion belongs.

## Writing the Contract Down

The three models form a coherent chain. Staging creates a joinable key. The first intermediate model uses that key to enrich with canonical meaning. The second pivots the result into an analytical schema while preserving unmapped rows for QA. From a design standpoint, the transformation logic is complete.

From a validation standpoint, we've asserted that it works without demonstrating it.

This is where test-driven thinking applies. The staging model makes a specific claim: given a `lab_provided_attribute`, `attribute_standardized` will be trimmed, lowercased, and have special characters replaced with underscores. That claim is precise enough to verify directly.

dbt's unit testing feature \[4\], introduced in version 1.8, lets you define expected transformation behavior in the model's YAML configuration. You provide mock input rows and declare the expected output rows. dbt runs the model against your mock inputs in isolation, without touching production data, and reports whether the actual output matches the declared expectations. The test is a machine-executable specification of what the model is supposed to do:

```yaml
unit_tests:
  - name: test_column_name_standardization
    description: "Check that column names are correctly trimmed, made lower case, and have symbols removed"
    model: stg_lab_samples_unpivoted
    given:
      - input: source('bronze', 'lab_samples_unpivoted')
        rows:
          - {lab_provided_attribute: ' extra spaces '}
          - {lab_provided_attribute: 'CAPITALS'}
          - {lab_provided_attribute: '-a$lot(of)weird#symbols.why/vendors%why@!'}
    expect:
      rows:
        - {attribute_standardized: 'extra_spaces'}
        - {attribute_standardized: 'capitals'}
        - {attribute_standardized: 'a_lot_of_weird_symbols_why_vendors_why'}
```

Three test cases, three behaviors. The whitespace case verifies that leading and trailing spaces are removed. The capitalization case verifies that mixed or uppercase input produces lowercase output. The symbols case verifies that punctuation and special characters become underscores. Together, they define the contract that downstream models rely on when they join to the mapping table on `attribute_standardized`.

Let's run it.

## The Tests Fail

Two of the three cases fail:

```
Failure in unit_test test_column_name_standardization

actual differs from expected:

@@ ,attribute_standardized
+++,_a_lot_of_weird_symbols_why_vendors_why__
+++,_extra_spaces_
---,a_lot_of_weird_symbols_why_vendors_why
   ,capitals
---,extra_spaces
```

`CAPITALS` → `capitals` passes. The other two do not.

Consider the whitespace case first. Input: `' extra spaces '`.
Expected output: `'extra_spaces'`.
Actual output: `'_extra_spaces_'`.

Trace through the cleaning chain. The functions nest inside each other, so they execute from the innermost outward: `TRANSLATE` runs first, `TRIM` runs second, `LOWER` runs third.

`TRANSLATE` replaces the eleven characters listed in its second argument with underscores. The character set is `'-$()#./ %@!'`. Count carefully: there is a space between the forward slash and the percent sign. Every space in the input string becomes an underscore. `' extra spaces '` becomes `'_extra_spaces_'`.

`TRIM` runs on the result of `TRANSLATE`. `TRIM` removes leading and trailing whitespace characters. There is no whitespace left; the leading and trailing spaces became underscores in the previous step. `TRIM` finds nothing to remove. The string stays `'_extra_spaces_'`.

`LOWER` runs. No change; the string is already lowercase.

The bug is an ordering problem. `TRIM` must run before `TRANSLATE`. Strip the whitespace first, then translate special characters, and the leading and trailing spaces are gone before `TRANSLATE` ever encounters them. The fix is a one-line change to the nesting order. The error is the kind that looks correct on first read because each individual function is reasonable, and the problem only becomes apparent when you trace a specific input through the full execution sequence.

The symbols case fails for related reasons, and tracing through it is left as an exercise. The root cause is the same.

Incidentally, I got this wrong the first time. The test told me.

Here is the uncomfortable implication of that fact. We have one unit test, and it covers the staging model. The two intermediate models have no tests at all. As I was working through this post, I noticed that the join in `int_lab_samples_standardized` was initially written on `row_index` and `vendor_id` alone, which would produce incorrect metadata associations whenever a vendor sends more than one file, since `row_index` resets to zero at the start of each file. I caught it by thinking carefully about what the columns actually represent. I would not have caught it by looking at the SQL and deciding it seemed reasonable.

That bug would have existed in the code whether the models were written with AI assistance, generated from a prompt and pasted in, or typed out manually by an engineer who had just had a very productive morning. "Looks correct" is not a testing strategy. The staging model's unit test demonstrates the alternative: make the claim explicit, make it machine-verifiable, and find out whether the claim is true before production data depends on it.

The question of how to extend that discipline to the intermediate models, and to the outputs of the entire pipeline against real data, is where Part 5 begins.

## What Silver Looks Like Now, and What Comes Next

The architecture is sound. Three models form a coherent transformation chain: staging creates the contract, the first intermediate model uses it to enrich, the second pivots the result into an analytical schema while preserving diagnostic signals about unmapped rows. The conceptual design reflects the same staging-and-integration-layer separation that Kimball described in the 1990s, implemented in dbt with explicit dependency management and inline documentation.

The first unit test does not pass, and the intermediate models have no tests at all. A unit test validates transformation logic in isolation — given these specific mock inputs, does the model produce these specific outputs? That kind of test is necessary. It is not sufficient to validate that a production pipeline is producing correct results.

How do we know that the actual vendor data flowing through this pipeline maps correctly to canonical columns? How do we validate that the mapping table covers the attributes vendors actually send? How do we catch a new analysis package that introduces column names we haven't seen?

Those questions require validation against real data, against statistical expectations, at the boundaries where data moves between layers. That's the subject of Part 5.

---

**Complete working example:** The dbt models described in this post are in [src/crucible/models/silver/](../src/crucible/models/silver/). The staging model, both intermediate models, and the unit test definition are all in that directory.

**References:**

\[1\] dbt Labs. (2024). *How we structure our dbt projects*. https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview

\[2\] dbt Labs. (2024). *Staging: Preparing and cleaning source data*. https://docs.getdbt.com/guides/best-practices/how-we-structure/2-staging

\[3\] Kimball, R., & Ross, M. (2013). *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling* (3rd ed.). Wiley.

\[4\] dbt Labs. (2024). *Unit tests*. https://docs.getdbt.com/docs/build/unit-tests
