create or replace view
    default_globalx.etl_integrated_etf_data
  as
    

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
    realtime_start,
    realtime_end,
    unemployment_rate
from default_globalx.integrated_etf_data
