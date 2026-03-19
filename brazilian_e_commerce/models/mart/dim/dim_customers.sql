{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    customer_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from {{ ref('stg_customers') }}