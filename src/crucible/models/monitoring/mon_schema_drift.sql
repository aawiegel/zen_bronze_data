{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'lab_samples_unpivoted') }}
),

-- Assign a chronological rank to each distinct file per vendor.
-- DENSE_RANK handles ties (same ingestion_timestamp) gracefully.
file_ranks AS (
    SELECT
        vendor_id,
        file_name,
        ingestion_timestamp,
        DENSE_RANK() OVER (
            PARTITION BY vendor_id
            ORDER BY ingestion_timestamp
        ) AS file_rank
    FROM source
    GROUP BY vendor_id, file_name, ingestion_timestamp
),

-- Build the unique attribute set per (vendor, file_rank).
file_attributes AS (
    SELECT DISTINCT
        file_ranks.vendor_id,
        file_ranks.file_name,
        file_ranks.ingestion_timestamp,
        file_ranks.file_rank,
        source.lab_provided_attribute
    FROM source
    INNER JOIN file_ranks
        ON  source.vendor_id           = file_ranks.vendor_id
        AND source.file_name           = file_ranks.file_name
        AND source.ingestion_timestamp = file_ranks.ingestion_timestamp
),

-- Self-join current file (rank N) against previous file (rank N-1).
-- FULL OUTER JOIN so rank-1 files (no prior) appear with NULLs on the prev side,
-- and removed attributes (absent from current) appear with NULLs on the current side.
drift AS (
    SELECT
        COALESCE(current_file.vendor_id,              previous_file.vendor_id)              AS vendor_id,
        current_file.file_name                                                               AS current_file_name,
        current_file.ingestion_timestamp                                                     AS current_ingestion_timestamp,
        current_file.file_rank                                                               AS current_file_rank,
        previous_file.file_name                                                              AS previous_file_name,
        previous_file.ingestion_timestamp                                                    AS previous_ingestion_timestamp,
        COALESCE(current_file.lab_provided_attribute, previous_file.lab_provided_attribute) AS lab_provided_attribute,
        CASE
            WHEN previous_file.lab_provided_attribute IS NULL THEN 'new'
            WHEN current_file.lab_provided_attribute  IS NULL THEN 'removed'
        END AS change_type
    FROM file_attributes AS current_file
    FULL OUTER JOIN file_attributes AS previous_file
        ON  current_file.vendor_id              = previous_file.vendor_id
        AND current_file.lab_provided_attribute = previous_file.lab_provided_attribute
        AND current_file.file_rank              = previous_file.file_rank + 1
    -- Only emit rows where an attribute changed.
    -- Stable attributes match on both sides and are excluded here.
    WHERE current_file.lab_provided_attribute  IS NULL
       OR previous_file.lab_provided_attribute IS NULL
)

SELECT
    vendor_id,
    current_file_name,
    current_ingestion_timestamp,
    current_file_rank,
    previous_file_name,
    previous_ingestion_timestamp,
    lab_provided_attribute,
    change_type
FROM drift
ORDER BY vendor_id, current_file_rank, change_type, lab_provided_attribute
