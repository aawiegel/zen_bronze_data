{{ config(materialized='view') }}

WITH customer_samples AS (
    SELECT * FROM {{ source('bronze', 'customer_samples') }}
),

customer_samples_staged AS (
    SELECT
        barcode AS sample_barcode,
        customer_id,
        crop_type,
        CAST(sample_date AS DATE) AS sample_date
    FROM customer_samples
)

SELECT * FROM customer_samples_staged
