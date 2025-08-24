with valuations_ranked as (
  select
    fund_name,
    event_date,
    transaction_amount,
    row_number() over (
      partition by fund_name, event_date
      order by transaction_amount desc  -- assumption: keep the highest valuation that day
    ) as rn
  from {{ ref('fund_events_v') }}
  where transaction_type = 'VALUATION'
),
valuations as (
  select fund_name, event_date, transaction_amount as valuation_amt
  from valuations_ranked
  qualify rn = 1
),
valuations_with_next as (
  select
    fund_name,
    event_date,
    valuation_amt,
    lead(event_date) over (partition by fund_name order by event_date) as next_event_date
  from valuations
),
flows as (
  select
    fund_name,
    event_date,
    case
      when transaction_type = 'CALL' then transaction_amount
      when transaction_type = 'DISTRIBUTION' then -transaction_amount
    end as cash_flow
  from {{ ref('fund_events_v') }}
  where transaction_type in ('CALL','DISTRIBUTION')
),
nav_points as (
  select
    v.fund_name,
    d.date as nav_date,
    v.valuation_amt
      + coalesce(
          sum(f.cash_flow) over (
            partition by v.fund_name, v.event_date
            order by d.date
            rows between unbounded preceding and current row
          ),
          0
        ) as nav
  from valuations_with_next v
  join {{ ref('dim_date') }} d
    on d.date >= v.event_date
   and d.date < coalesce(v.next_event_date, to_date('2999-12-31'))
  left join flows f
    on f.fund_name = v.fund_name
   and f.event_date = d.date
)
select fund_name, nav_date, nav
from nav_points
order by fund_name, nav_date
