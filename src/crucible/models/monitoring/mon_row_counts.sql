{{ config(materialized='view') }}

-- One row per ingestion event. Use this to track sample flow over time and
-- flag unexpected volume drops or spikes against what the vendor committed to send.
SELECT
    vendor_id,
    file_name,
    ingestion_timestamp,
    COUNT(*)                            AS total_rows,
    COUNT(DISTINCT row_index)           AS total_samples,
    COUNT(DISTINCT lab_provided_attribute) AS distinct_attributes
FROM {{ source('bronze', 'lab_samples_unpivoted') }}
GROUP BY vendor_id, file_name, ingestion_timestamp
ORDER BY vendor_id, ingestion_timestamp DESC
