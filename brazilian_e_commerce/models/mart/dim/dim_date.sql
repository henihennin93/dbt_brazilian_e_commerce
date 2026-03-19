{{ config(materialized='table') }}

with dates as (

    select distinct cast(order_purchase_timestamp as date) as date_day
    from {{ ref('stg_orders') }}
    where order_purchase_timestamp is not null

    union

    select distinct cast(order_delivered_customer_date as date) as date_day
    from {{ ref('stg_orders') }}
    where order_delivered_customer_date is not null

    union

    select distinct cast(order_estimated_delivery_date as date) as date_day
    from {{ ref('stg_orders') }}
    where order_estimated_delivery_date is not null

)

select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as order_date_key,
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
    date_day,
    extract(year from date_day) as year_number,
    extract(quarter from date_day) as quarter_number,
    extract(month from date_day) as month_number,
    extract(day from date_day) as day_number,
    monthname(date_day) as month_name,
    dayname(date_day) as weekday_name
from dates