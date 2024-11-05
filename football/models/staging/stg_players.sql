with source as (
    select *,
           current_timestamp as processed_at
    from {{ source('raw_data', 'raw_players') }}
)

select * from source
