{{ config(
    materialized='incremental'
) }}

with source as (
    select *,
           current_timestamp as processed_at
    from {{ source('raw_data', 'raw_transfers') }}
)

select * from source
{% if is_incremental() %}
where transfer_date >= (
    select coalesce(max(transfer_date), '1900-01-01') from {{this}}
)
{% endif %}
