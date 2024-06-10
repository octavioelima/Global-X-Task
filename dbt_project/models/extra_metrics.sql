{{ config(
    materialized='view',
    pre_hook=drop_view(this)
) }}

with fred as (
    select
        date,
        unemployment_rate
    from {{ ref('fred_unemp') }}
),

polygon as (
    select
        date,
        volume,
        volume_weighted_avg_price,
        opening_price,
        close_price,
        highest_price,
        lowest_price,
        timestamp,
        number_of_trades,
        symbol
    from {{ ref('polygon') }}
),

joined_data as (
    select
        p.date,
        p.volume,
        p.volume_weighted_avg_price,
        p.opening_price,
        p.close_price,
        p.highest_price,
        p.lowest_price,
        p.timestamp,
        p.number_of_trades,
        p.symbol,
        f.unemployment_rate
    from
        polygon p
    left join
        fred f
    on
        p.date = f.date
)

select
    jd.date,
    jd.symbol,
    jd.volume,
    jd.volume_weighted_avg_price,
    jd.opening_price,
    jd.close_price,
    jd.highest_price,
    jd.lowest_price,
    jd.number_of_trades,
    -- Calculate daily_return as the percentage change from the previous day's close_price to today's close_price
    (jd.close_price - LAG(jd.close_price) OVER (PARTITION BY jd.symbol ORDER BY jd.date)) / LAG(jd.close_price) OVER (PARTITION BY jd.symbol ORDER BY jd.date) as daily_return,
    -- Calculate moving_avg_50 as the 50-day moving average of close_price
    AVG(jd.close_price) OVER (PARTITION BY jd.symbol ORDER BY jd.date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as moving_avg_50,
    -- Calculate Change in Price
    (jd.close_price - jd.opening_price) as price_change,
    (jd.highest_price - jd.lowest_price) / jd.lowest_price * 100 as intraday_volatility
from
    joined_data jd
