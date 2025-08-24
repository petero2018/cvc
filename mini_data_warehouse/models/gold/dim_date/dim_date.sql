with bounds as (
  select to_date('2020-01-01') as start_dt,
         to_date('2035-12-31') as end_dt
),
dates as (
  select
    dateadd(day, seq4(), (select start_dt from bounds))::date as dt
  from table(generator(rowcount => 60000))  -- literal big enough for 2020–2035 (~5.9k days)
)
select
  to_number(to_char(dt,'YYYYMMDD')) as date_id,
  dt as date,
  year(dt)      as year,
  quarter(dt)   as quarter,
  month(dt)     as month,
  day(dt)       as day_of_month,
  dayofweek(dt) as day_of_week
from dates
where dt <= (select end_dt from bounds)
order by dt
