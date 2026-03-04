{{ config(materialized='view') }}

WITH int_lab_samples_standardized AS (
    SELECT * FROM {{ ref('int_lab_samples_standardized') }}
),

stg_customer_samples AS (
    SELECT * FROM {{ ref('stg_customer_samples') }}
),

stg_customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

lab_samples_with_customers AS (
    SELECT
        int_lab_samples_standardized.*,
        stg_customer_samples.sample_barcode IS NOT NULL AS has_customer_assignment,
        stg_customer_samples.customer_id,
        stg_customer_samples.crop_type,
        stg_customer_samples.sample_date,
        stg_customers.customer_name,
        stg_customers.date_of_birth,
        stg_customers.age,
        stg_customers.email,
        stg_customers.phone,
        stg_customers.street_address,
        stg_customers.city,
        stg_customers.state,
        stg_customers.zip_code,
        stg_customers.notes
    FROM int_lab_samples_standardized
    LEFT JOIN stg_customer_samples
        ON int_lab_samples_standardized.sample_barcode = stg_customer_samples.sample_barcode
    LEFT JOIN stg_customers
        ON stg_customer_samples.customer_id = stg_customers.customer_id
)

SELECT * FROM lab_samples_with_customers
