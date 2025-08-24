
with valuations_ranked as (
  select
    fund_name,
    transaction_date,
    transaction_amount,
    row_number() over (
      partition by fund_name, transaction_date
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
valuations_with_next as (
  select
    fund_name,
    transaction_date,
    valuation_amt,
    lead(transaction_date) over (partition by fund_name order by transaction_date) as next_transaction_date
  from valuations
),
flows as (
  select
    fund_name,
    transaction_date,
    sum(case
      when upper(transaction_type) = 'CALL' then try_to_number(transaction_amount)
      when upper(transaction_type) = 'DISTRIBUTION' then -try_to_number(transaction_amount)
      else 0 end) as cash_flow
  from {{ ref('stg_fund_data') }}
  where upper(transaction_type) in ('CALL','DISTRIBUTION')
  group by 1,2
),
nav_points as (
  select
    v.fund_name,
    d.date as nav_date,
    v.valuation_amt
    + coalesce(
        sum(f.cash_flow) over (
          partition by v.fund_name, v.transaction_date
          order by d.date
          rows between unbounded preceding and current row
        ),
        0
      ) as nav
  from valuations_with_next v
  join {{ ref('dim_date') }} d
    on d.date >= v.transaction_date
   and d.date < coalesce(v.next_transaction_date, to_date('2999-12-31'))
  left join flows f
    on f.fund_name = v.fund_name
   and f.transaction_date = d.date
)
select fund_name, nav_date, nav
from nav_points
order by fund_name, nav_date
