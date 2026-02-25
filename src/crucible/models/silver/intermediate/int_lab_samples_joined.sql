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
