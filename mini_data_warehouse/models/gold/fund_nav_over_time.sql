with valuations as (
  select fund_name, event_date, transaction_amount,
         row_number() over (partition by fund_name, event_date
                            order by transaction_index nulls last) as rn
  from {{ ref('fund_events_v') }}
  where transaction_type = 'VALUATION'
),
latest_valuation_per_date as (
  select fund_name, event_date, transaction_amount as valuation_amt
  from valuations
  qualify rn = 1
),
flows as (
  select fund_name, event_date,
         case when transaction_type in ('CALL') then transaction_amount
              when transaction_type in ('DISTRIBUTION') then -transaction_amount
         end as cash_flow
  from {{ ref('fund_events_v') }}
  where transaction_type in ('CALL','DISTRIBUTION')
),
nav_points as (
  -- NAV on each valuation date + any flow dates after that valuation until the next valuation
  select
    f.fund_name,
    d.dt as nav_date,
    v.valuation_amt
      + coalesce( sum(f2.cash_flow)
          over (partition by f.fund_name, v.event_date
                order by d.dt
                rows between unbounded preceding and current row), 0) as nav
  from latest_valuation_per_date v
  join (select distinct fund_name from {{ ref('fund_events_v') }}) f using (fund_name)
  join lateral (
    -- build the window between this valuation date and next valuation date for that fund
    select dt
    from {{ ref('dim_date') }} d
    where d.dt >= v.event_date
      and d.dt < coalesce( lead(v.event_date)
                           over (partition by v.fund_name order by v.event_date),
                           '2999-12-31'::date )
  ) d
  left join flows f2
    on f2.fund_name = v.fund_name and f2.event_date = d.dt
)
select fund_name, nav_date, nav
from nav_points
qualify nav is not null
order by fund_name, nav_date;
