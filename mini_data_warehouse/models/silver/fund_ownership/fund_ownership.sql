with commits as (
  select
    fund_name,
    transaction_date,
    try_to_number(transaction_amount) as amount
  from {{ ref('stg_fund_data') }}
  where upper(transaction_type) = 'COMMITMENT'
),
fund_size_latest as (
  select fund_name, max(try_to_number(fund_size)) as fund_size
  from {{ ref('stg_fund_data') }}
  group by 1
),
asof_dates as (
  -- date spine per fund bounded to its activity window
  select
    c.fund_name,
    d.date
  from commits c
  join {{ ref('dim_date') }} d
    on d.date >= (select min(transaction_date) from commits where fund_name = c.fund_name)
   and d.date <= (select max(transaction_date) from {{ ref('stg_fund_data') }} where fund_name = c.fund_name)
  group by 1,2
),
cum_commits as (
  select
    a.fund_name,
    a.date as asof_date,
    sum(case when c.transaction_date <= a.date then c.amount else 0 end) as cum_commit
  from asof_dates a
  left join commits c
    on c.fund_name = a.fund_name
  group by 1,2
)
select
  cc.fund_name,
  cc.asof_date,
  cc.cum_commit / nullif(fsl.fund_size, 0) as ownership_pct
from cum_commits cc
join fund_size_latest fsl using (fund_name)