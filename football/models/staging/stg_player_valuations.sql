{{ config(
    materialized='incremental'
) }}

with source as (
    select *,
           current_timestamp as processed_at
    from {{ source('raw_data', 'raw_player_valuations') }}
)

select * from source
{% if is_incremental() %}
where date >= (
    select coalesce(max(date), '1900-01-01') from {{this}}
)
{% endif %}