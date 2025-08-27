with commits as (
  select
    fund_name,
    transaction_date         as tx_date,        -- already DATE in stg
    transaction_amount       as amount
  from {{ ref('stg_fund_data') }}
  where upper(transaction_type) = 'COMMITMENT'
),

fund_size_latest as (
  select fund_name, max(fund_size) as fund_size
  from {{ ref('stg_fund_data') }}
  group by 1
),

/*
Picks the latest size per fund (per requirement: “fund_size represents the latest size of the fund, not the size at transaction time”).
*/

-- aggregate commitments by fund+day
commits_by_day as (
  select fund_name, tx_date as dt, sum(amount) as amount
  from commits
  group by 1,2
),

/*
Aggregates multiple COMMITMENT rows on the same day:
1 row per (fund_name, date) with sum(amount).
*/


-- build a date spine per fund over its active window
fund_dates as (
  select
    f.fund_name,
    d.date as dt
  from (select distinct fund_name from {{ ref('stg_fund_data') }}) f
  join {{ ref('dim_date') }} d
    on d.date between
       (select coalesce(min(tx_date), current_date) from commits c where c.fund_name = f.fund_name)
       and
       (select coalesce(max(tx_date), current_date) from commits c where c.fund_name = f.fund_name)
),

-- cumulative commitments per fund over time
cum as (
  select
    fd.fund_name,
    fd.dt as asof_date,
    sum(coalesce(cbd.amount, 0)) over (
      partition by fd.fund_name
      order by fd.dt
      rows between unbounded preceding and current row
    ) as cum_commit
  from fund_dates fd
  left join commits_by_day cbd
    on cbd.fund_name = fd.fund_name
   and cbd.dt       = fd.dt
)

-- ownership_pct  calculation

select
  c.fund_name,
  c.asof_date,
  c.cum_commit / nullif(f.fund_size, 0) as ownership_pct
from cum c
join fund_size_latest f
  on f.fund_name = c.fund_name