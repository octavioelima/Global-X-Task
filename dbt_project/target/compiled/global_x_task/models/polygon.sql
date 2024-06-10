-- models/polygon.sql



with integrated_data as (
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
    from default_globalx.etl_integrated_etf_data
)

select * from integrated_data