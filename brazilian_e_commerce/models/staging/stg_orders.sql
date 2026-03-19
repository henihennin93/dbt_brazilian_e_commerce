{{ config(materialized='table') }}
select *
from BRAZILIAN_E_COMMERCE.CURATED_TABLES_RAW_DATA.OLIST_ORDERS_DATASET