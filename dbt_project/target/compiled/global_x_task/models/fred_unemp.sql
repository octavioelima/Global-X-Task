-- models/fred_unemp.sql



with integrated_data as (
    select
        date,
        unemployment_rate
    from default_globalx.etl_integrated_etf_data
    where unemployment_rate is not null
)

select * from integrated_data