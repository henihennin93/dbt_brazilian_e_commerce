{{ config(
    materialized='incremental',
    unique_key='fact_order_item_key'
) }}

with order_items as (

    select *
    from {{ ref('stg_order_items') }}

),

orders as (

    select *
    from {{ ref('stg_orders') }}

    {% if is_incremental() %}
    where order_purchase_timestamp > (
        select coalesce(max(order_purchase_timestamp), '1900-01-01')
        from {{ this }}
    )
    {% endif %}

),

customers as (

    select *
    from {{ ref('dim_customers') }}

),

products as (

    select *
    from {{ ref('dim_products') }}

),

sellers as (

    select *
    from {{ ref('dim_sellers') }}

),

payments as (

    select *
    from {{ ref('agg_payment') }}

),

reviews as (

    select *
    from {{ ref('agg_reviews') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['order_items.order_id', 'order_items.order_item_id']) }} as fact_order_item_key,
        order_items.order_id,
        order_items.order_item_id,
        customers.customer_key,
        products.product_key,
        sellers.seller_key,
        to_number(to_char(cast(orders.order_purchase_timestamp as date), 'YYYYMMDD')) as order_date_key,
        orders.order_purchase_timestamp,
        order_items.price,
        order_items.freight_value,
        payments.sum_payment_value,
        reviews.avg_review_score,
        current_timestamp() as loaded_at
    from order_items
    inner join orders
        on order_items.order_id = orders.order_id
    left join customers
        on orders.customer_id = customers.customer_id
    left join products
        on order_items.product_id = products.product_id
    left join sellers
        on order_items.seller_id = sellers.seller_id
    left join payments
        on order_items.order_id = payments.order_id
    left join reviews
        on order_items.order_id = reviews.order_id

)

select *
from final