{{
    config(
        materialized='incremental'
    )
}}

with source as (

    SELECT
        *,
        CURRENT_TIMESTAMP as processed_at
    FROM
        {{ source('raw_data', 'raw_games') }}
)

SELECT
    *
FROM source
{% if is_incremental() %}
WHERE date >= (
    SELECT coalesce(max(date), '1900-01-01') FROM {{ this }}
)
{% endif %}
