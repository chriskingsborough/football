with source as (
    SELECT
        club_id,
        club_code,
        name,
        domestic_competition_id,
        total_market_value,
        squad_size,
        average_age,
        foreigners_number,
        foreigners_percentage,
        national_team_players,
        stadium_name,
        stadium_seats,
        net_transfer_record,
        coach_name,
        last_season,
        url,
        CURRENT_TIMESTAMP as processed_at
    FROM {{ source('raw_data', 'raw_clubs') }}
)
select * from source