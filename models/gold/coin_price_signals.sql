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
    symbol,
    current_price,
    last_updated_at,
    MAX(current_price) OVER (
        PARTITION BY coin
        ORDER BY last_updated_at
        RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
    ) AS max_price_7d
FROM top_20_coins
ORDER BY coin, last_updated_at;

