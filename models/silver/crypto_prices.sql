{{ config(
    materialized='table'
    )
}}

SELECT
    raw_data:name::STRING AS coin,
    raw_data:symbol::STRING AS symbol,
    raw_data:current_price::FLOAT AS current_price,
    raw_data:market_cap::FLOAT AS market_cap,
    raw_data:market_cap_rank::INT AS rank,
    CAST(raw_data:total_volume::STRING AS BIGINT) AS volume,
    raw_data:high_24h::FLOAT AS high_24hr,
    raw_data:low_24h::FLOAT AS low_24hr,
    raw_data:last_updated::TIMESTAMP as last_updated_at
FROM
    crypto_bronze_raw
