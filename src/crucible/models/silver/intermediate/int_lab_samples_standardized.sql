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
