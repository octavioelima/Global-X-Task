-- models/integrated_etf_data.sql
select
    date,
    index,
    volume,
    volume_weighted_avg_price,
    opening_price,
    close_price,
    highest_price,
    lowest_price,
    timestamp,
    number_of_trades,
    symbol,
    daily_return,
    moving_avg_50,
    realtime_start,
    realtime_end,
    unemployment_rate
from globalx.integrated_etf_data