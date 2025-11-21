{{ config(
    materialized='view'
    )
}}

WITH top_20_coins AS (
    SELECT *
    FROM crypto_prices
    WHERE rank <= 20
)
SELECT
    coin,
    rank,
    current_price,
    last_updated_at,
    MAX(high_24hr) OVER (
        PARTITION BY coin
        ORDER BY last_updated_at
        RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
    ) AS max_price_7d,
    MIN(low_24hr) OVER (
        PARTITION BY coin
        ORDER BY last_updated_at
        RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
    ) AS min_price_7d
FROM top_20_coins
ORDER BY coin, last_updated_at;

