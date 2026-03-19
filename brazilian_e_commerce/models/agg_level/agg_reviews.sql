{{ config(materialized='table') }}

with reviews as (

    select *
    from {{ ref('stg_reviews') }}

),

aggregated as (

    select
        {{ dbt_utils.generate_surrogate_key(['order_id', 'review_id']) }} as review_key,
        order_id,
        count(*) as review_row_count,
        avg(review_score) as avg_review_score,
        min(review_score) as min_review_score,
        max(review_score) as max_review_score,
        min(review_creation_date) as first_review_creation_date,
        max(review_creation_date) as last_review_creation_date,
        min(review_answer_timestamp) as first_review_answer_timestamp,
        max(review_answer_timestamp) as last_review_answer_timestamp
    from reviews
    group by order_id,  {{ dbt_utils.generate_surrogate_key(['order_id', 'review_id']) }}


)

select *
from aggregated