-- Compile-time params (can be overridden via vars)
{% set start_str = var('date_start', '2020-01-01') %}
{% set end_str   = var('date_end',   '2035-12-31') %}
{% set fiscal_start_month = var('fiscal_year_start_month', 1) | int %}

-- Precompute rowcount literal for generator
{% set days = (modules.datetime.datetime.fromisoformat(end_str)
             - modules.datetime.datetime.fromisoformat(start_str)).days + 1 %}

with bounds as (
  select to_date('{{ start_str }}') as start_dt,
         to_date('{{ end_str }}')   as end_dt
),
base as (
  select
    dateadd(day, seq4(), (select start_dt from bounds))::date as date
  from table(generator(rowcount => {{ days }}))
),
gregorian as (
  select
    date,
    to_number(to_char(date, 'YYYYMMDD')) as date_id,
    year(date)                           as year,
    month(date)                          as month,
    day(date)                            as day_of_month,
    quarter(date)                        as quarter,
    dayofweekiso(date)                   as iso_dow,        -- 1=Mon..7=Sun
    weekiso(date)                        as iso_week,
    last_day(date, 'month')              as month_end_date,
    last_day(date, 'quarter')            as quarter_end_date,
    last_day(date, 'year')               as year_end_date,
    dateadd(day, 1 - dayofweekiso(date), date) as week_monday,   -- week start (Mon)
    date_trunc('month', date)            as month_start_date,
    date_trunc('quarter', date)          as quarter_start_date,
    date_trunc('year', date)             as year_start_date
  from base
),
fiscal as (
  -- Fiscal year/quarter from a chosen start month
  select
    g.*,
    case
      when month >= {{ fiscal_start_month }}
        then year
      else year - 1
    end                                          as fiscal_year,
    1 + floor(mod(month - {{ fiscal_start_month }}, 12) / 3)     as fiscal_quarter,
    add_months(date_trunc('year',
              to_date(to_char(
                case when month >= {{ fiscal_start_month }} then year else year - 1 end
              ) || '-' || lpad('{{ fiscal_start_month }}', 2, '0') || '-01')), 0) as fiscal_year_start_date,
    last_day(add_months(
      add_months(date_trunc('year',
              to_date(to_char(
                case when month >= {{ fiscal_start_month }} then year else year - 1 end
              ) || '-' || lpad('{{ fiscal_start_month }}', 2, '0') || '-01')), 0),
      11), 'month') as fiscal_year_end_date
  from gregorian g
),
flags as (
  select
    f.*,
    -- Gregorian flags
    case when date = month_end_date   then true else false end as is_month_end,
    case when date = quarter_end_date then true else false end as is_quarter_end,
    case when iso_dow in (6,7)        then true else false end as is_weekend,
    -- Fiscal flags
    case
      when date = last_day(add_months(fiscal_year_start_date, floor(mod(month - {{ fiscal_start_month }}, 12) / 1)), 'month')
      then true else false end as is_fiscal_month_end,
    case
      when date = last_day(add_months(fiscal_year_start_date, fiscal_quarter * 3 - 1), 'month')
      then true else false end as is_fiscal_quarter_end
  from fiscal f
),
holidays as (
  {% if var('include_holidays', false) %}
    select distinct to_date(holiday_date) as holiday_date
    from {{ ref('holidays') }}
  {% else %}
    select to_date(null) as holiday_date
    where false
  {% endif %}
),
with_holidays as (
  select
    fl.*,
    case when exists (
      select 1 from holidays h where h.holiday_date = fl.date
    ) then true else false end as is_holiday
  from flags fl
),
business as (
  select
    wh.*,
    case when wh.is_weekend or wh.is_holiday then false else true end as is_business_day
  from with_holidays wh
),
business_rank as (
  -- Nth business day within month; last business day flag
  select
    b.*,
    case when is_business_day
      then row_number() over (partition by year, month order by date)
      else null end as business_day_of_month,
    max(case when is_business_day then date end)
      over (partition by year, month) as last_business_day_in_month
  from business b
)
select
  date_id,
  date,
  year, quarter, month, day_of_month, iso_week, iso_dow,
  week_monday,
  month_start_date, month_end_date,
  quarter_start_date, quarter_end_date,
  year_start_date, year_end_date,
  fiscal_year, fiscal_quarter, fiscal_year_start_date, fiscal_year_end_date,
  is_month_end, is_quarter_end, is_fiscal_month_end, is_fiscal_quarter_end,
  is_weekend, is_holiday, is_business_day,
  business_day_of_month,
  case when date = last_business_day_in_month then true else false end as is_last_business_day_of_month
from business_rank
order by date
