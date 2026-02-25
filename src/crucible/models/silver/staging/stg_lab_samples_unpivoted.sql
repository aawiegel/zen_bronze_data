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