{{ config(materialized='table') }}

with dates as (
  select dateadd(day, row_number() over() - 1, '2020-01-01') as date
  from table(generator(rowcount => 1000))  -- ~3 years of dates
)
select
  to_char(date, 'YYYYMMDD')::int as date_id,
  date,
  year(date) as year,
  quarter(date) as quarter,
  month(date) as month,
  day(date) as day_of_month,
  dayofweek(date) as day_of_week
from dates
where date <= '2025-12-31'
