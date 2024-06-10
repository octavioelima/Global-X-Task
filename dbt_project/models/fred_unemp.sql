-- models/fred_unemp.sql

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
        unemployment_rate
    from {{ ref('etl_integrated_etf_data') }}
    where unemployment_rate is not null
)

select * from integrated_data