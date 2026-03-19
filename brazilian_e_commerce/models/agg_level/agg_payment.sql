{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'payment_type']) }} as payment_id,
order_id,
payment_type,
count(payment_sequential) as total_count_payment,
sum(payment_installments) as sum_payment_installments,
sum(payment_value) as sum_payment_value
from {{ ref('stg_payment') }}
group by order_id ,payment_type


