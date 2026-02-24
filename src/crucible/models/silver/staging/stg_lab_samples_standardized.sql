{{ config(materialized='table') }}

SELECT
    bronze.row_index,
    bronze.column_index,
    bronze.lab_provided_attribute,
    bronze.lab_provided_value,
    bronze.vendor_id,
    bronze.file_name,
    bronze.ingestion_timestamp,
    canonical.canonical_column_id,
    canonical.canonical_column_name,
    canonical.column_category,
    canonical.data_type
FROM {{ source('bronze', 'lab_samples_unpivoted') }} AS bronze
LEFT JOIN {{ source('bronze', 'vendor_column_mapping') }} AS mapping
    ON bronze.lab_provided_attribute = mapping.vendor_column_name
    AND bronze.vendor_id = mapping.vendor_id
LEFT JOIN {{ source('silver', 'canonical_column_definitions') }} AS canonical
    ON mapping.canonical_column_id = canonical.canonical_column_id
