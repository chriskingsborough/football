with source as (
    select *,
           current_timestamp as processed_at
    from {{ source('raw_data', 'raw_competitions') }}
)

select * from source
