create or replace view
    default_globalx.group_by_stock
  as
    

with joined_data as (
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
        default_globalx.polygon p
    left join
        default_globalx.fred_unemp f
    on
        p.date = f.date
),

aggregated_data as (
    select
        symbol,
        AVG(volume) as avg_volume,
        AVG(close_price) as avg_close_price,
        AVG(opening_price) as avg_opening_price,
        AVG(highest_price) as avg_highest_price,
        AVG(lowest_price) as avg_lowest_price,
        AVG(unemployment_rate) as avg_unemployment_rate,
        COUNT(*) as record_count
    from
        joined_data
    group by
        symbol
)

select
    symbol,
    avg_volume,
    avg_close_price,
    avg_opening_price,
    avg_highest_price,
    avg_lowest_price,
    avg_unemployment_rate,
    record_count
from
    aggregated_data
