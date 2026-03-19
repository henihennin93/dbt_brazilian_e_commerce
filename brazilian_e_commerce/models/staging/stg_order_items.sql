{{ config(materialized='table') }}

select *
from BRAZILIAN_E_COMMERCE.CURATED_TABLES_RAW_DATA.OLIST_ORDER_ITEMS_DATASET
where order_item_id=1
