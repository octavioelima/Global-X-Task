-- models/polygon.sql

{{ config(
    materialized='table',
    pre_hook="""
        {% if this.is_table %}
            DROP TABLE IF EXISTS {{ this }};
        {% elif this.is_view %}
            DROP VIEW IF EXISTS {{ this }};
        {% endif %}
    """
) }}

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
    from {{ ref('etl_integrated_etf_data') }}
)

select * from integrated_data

