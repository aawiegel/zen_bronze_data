{{ config(materialized='view') }}

WITH joined AS (
    SELECT * FROM {{ ref('int_lab_samples_joined') }}
),

-- Unmapped rows surface as canonical_column_id IS NULL after the LEFT JOINs
-- in int_lab_samples_joined. Both the raw vendor name and the standardized form
-- are preserved so the dashboard shows exactly what the mapping table is missing.
unmapped AS (
    SELECT *
    FROM joined
    WHERE canonical_column_id IS NULL
)

SELECT
    vendor_id,
    lab_provided_attribute,
    attribute_standardized,
    COUNT(*)                    AS occurrence_count,
    COUNT(DISTINCT file_name)   AS distinct_files,
    MIN(ingestion_timestamp)    AS first_seen,
    MAX(ingestion_timestamp)    AS last_seen
FROM unmapped
GROUP BY
    vendor_id,
    lab_provided_attribute,
    attribute_standardized
ORDER BY vendor_id, occurrence_count DESC
