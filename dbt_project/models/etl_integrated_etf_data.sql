{{ config(
    materialized='view',
    pre_hook=drop_view(this)
) }}

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
from {{ source('global_x_task', 'integrated_etf_data') }}