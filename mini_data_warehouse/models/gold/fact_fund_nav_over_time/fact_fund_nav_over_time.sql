-- 1) One valuation per fund/day (tie-break: highest amount)
with valuations_ranked as (
  select
    trim(upper(fund_name)) as fund_name,
    transaction_date,
    transaction_amount as transaction_amount,
    row_number() over (
      partition by trim(upper(fund_name)), transaction_date
      order by transaction_amount desc
    ) as rn
  from {{ ref('stg_fund_data') }}
  where upper(transaction_type) = 'VALUATION'
),
valuations as (
  select fund_name, transaction_date, transaction_amount as valuation_amt
  from valuations_ranked
  qualify rn = 1
),

-- 2) Window from each valuation to (but not including) the next valuation
valuations_with_next as (
  select
    fund_name,
    transaction_date,
    valuation_amt,
    lead(transaction_date) over (partition by fund_name order by transaction_date) as next_transaction_date
  from valuations
),

-- 3) Daily cash flows (+ for CALL, - for DISTRIBUTION)
flows as (
  select
    trim(upper(fund_name)) as fund_name,
    transaction_date,
    sum(case
      when upper(transaction_type) = 'CALL'         then transaction_amount
      when upper(transaction_type) = 'DISTRIBUTION' then - transaction_amount
      else 0 end
    ) as cash_flow
  from {{ ref('stg_fund_data') }}
  where upper(transaction_type) in ('CALL','DISTRIBUTION')
  group by 1,2
),

-- 4) Dense daily NAV series per fund using dim_date
nav_points as (
  select
    v.fund_name,
    d.date as nav_date,
    v.transaction_date as valuation_date_anchor,
    v.valuation_amt
      + coalesce(
          sum(f.cash_flow) over (
            partition by v.fund_name, v.transaction_date
            order by d.date
            rows between unbounded preceding and current row
          ),
          0
        ) as nav,
    coalesce(
      sum(f.cash_flow) over (
        partition by v.fund_name, v.transaction_date
        order by d.date
        rows between unbounded preceding and current row
      ),
      0
    ) as flows_cum_from_anchor
  from valuations_with_next v
  join {{ ref('dim_date') }} d
    on d.date >= v.transaction_date
   and d.date < coalesce(v.next_transaction_date, to_date('2999-12-31'))
  left join flows f
    on f.fund_name = v.fund_name
   and f.transaction_date = d.date
),

-- 5) Look up dimension keys (text hash keys with '_UNKNOWN' sentinel)
lkp as (
  select
    n.*,
    coalesce(df.fund_key, '_UNKNOWN')                 as fund_key,
    coalesce(dd1.date_key, '_UNKNOWN')                as nav_date_key,
    coalesce(dd2.date_key, '_UNKNOWN')                as valuation_date_anchor_key
  from nav_points n
  left join {{ ref('dim_funds') }} df
    on df.fund_name = n.fund_name
  left join {{ ref('dim_date') }} dd1
    on dd1.date = n.nav_date
  left join {{ ref('dim_date') }} dd2
    on dd2.date = n.valuation_date_anchor
),

-- 6) Add a "current" flag (latest available NAV per fund)
with_current as (
  select
    l.*,
    case
      when l.nav_date = max(l.nav_date) over (partition by l.fund_name) then true
      else false
    end as is_current
  from lkp l
)

-- Final select (keep naturals for audit; facts use only keys downstream)
select
  fund_key,
  nav_date_key,
  valuation_date_anchor_key,
  -- optional naturals for traceability
  fund_name,
  nav_date,
  valuation_date_anchor,
  flows_cum_from_anchor,
  nav,
  is_current
from with_current
order by fund_name, nav_date