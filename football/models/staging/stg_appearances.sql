{{
    config(
        materialized='incremental'
    )
}}

SELECT 
    *,
    CURRENT_TIMESTAMP as processed_at
FROM {{ source('raw_data', 'raw_appearances') }}
{% if is_incremental() %}
WHERE date >= (
    SELECT coalesce(max(date), '1900-01-01') FROM {{ this }}
)
{% endif %}
