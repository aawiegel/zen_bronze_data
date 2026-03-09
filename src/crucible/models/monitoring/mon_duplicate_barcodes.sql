{{ config(materialized='view') }}

WITH standardized AS (
    SELECT * FROM {{ ref('int_lab_samples_standardized') }}
),

-- One row per (vendor_id, sample_barcode, file_name, row_index) establishes
-- the grain of "a sample appearance." Distinct on ingestion_timestamp so that
-- re-ingesting the same file does not inflate appearance counts.
sample_appearances AS (
    SELECT DISTINCT
        vendor_id,
        sample_barcode,
        file_name,
        row_index
    FROM standardized
    WHERE sample_barcode IS NOT NULL
),

-- A barcode is duplicated if it maps to more than one (file_name, row_index) pair.
-- distinct_files = 1 with total_appearances > 1 signals an intra-file duplicate.
-- distinct_files > 1 signals the same sample was submitted across multiple files.
barcode_summary AS (
    SELECT
        vendor_id,
        sample_barcode,
        COUNT(*)                    AS total_appearances,
        COUNT(DISTINCT file_name)   AS distinct_files,
        MIN(file_name)              AS first_file,
        MAX(file_name)              AS last_file
    FROM sample_appearances
    GROUP BY vendor_id, sample_barcode
    HAVING COUNT(*) > 1
)

SELECT
    vendor_id,
    sample_barcode,
    total_appearances,
    distinct_files,
    CASE
        WHEN distinct_files = 1 THEN 'intra_file'
        ELSE 'cross_file'
    END                             AS duplicate_type,
    first_file,
    last_file
FROM barcode_summary
ORDER BY vendor_id, total_appearances DESC
