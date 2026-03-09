{{ config(materialized='view') }}

WITH customers AS (
    SELECT * FROM {{ source('bronze', 'customers') }}
),

customers_staged AS (
    SELECT
        customer_id,
        customer_name,
        CAST(date_of_birth AS DATE) AS date_of_birth,
        CAST(age AS INTEGER) AS age,
        email,
        phone,
        street_address,
        city,
        state,
        zip_code,
        notes
    FROM customers
)

SELECT * FROM customers_staged
