with source as (
    select 
        *,
        CURRENT_TIMESTAMP as processed_at
    from 
        {{source('raw_data', 'raw_club_games')}}
)

select 
    *
from source