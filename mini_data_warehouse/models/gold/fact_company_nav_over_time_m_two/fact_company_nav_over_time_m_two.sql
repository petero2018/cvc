with cov as (
  select
    trim(upper(fund_name))   as fund_name,
    trim(upper(company_id))  as company_id,
    company_name,
    valuation_date,
    valuation_amount
  from {{ ref('company_valuations_v') }}
),

fund_nav as (
  select
    trim(upper(fund_name)) as fund_name,
    nav_date,
    nav
  from {{ ref('fact_fund_nav_over_time') }}
),

--Aggregates company valuations to sum(company valuations) per (fund, date)
totals as (
  select
    fund_name,
    valuation_date,
    sum(valuation_amount) as total_company_val
  from cov
  group by 1,2
),

--Computes the date-level scaling facto
scale as (
  select
    t.fund_name,
    t.valuation_date,
    case when t.total_company_val = 0 then null
         else f.nav / t.total_company_val end as scale_to_fund
  from totals t
  join fund_nav f
    on f.fund_name = t.fund_name
   and f.nav_date  = t.valuation_date
),

--Applies the scale to each company valuation on that date:
base as (
  select
    c.fund_name,
    c.company_id,
    c.company_name,
    c.valuation_date as nav_date,
    c.valuation_amount,
    s.scale_to_fund,
    CAST((c.valuation_amount * s.scale_to_fund)  AS DECIMAL(18,2)) as company_nav
  from cov c
  join scale s
    on s.fund_name      = c.fund_name
   and s.valuation_date = c.valuation_date
),

-- Dimension lookups (LEFT JOIN + sentinel fallback)
lkp as (
  select
    b.*,
    coalesce(df.fund_key,    '_UNKNOWN') as fund_key,
    coalesce(dc.company_key, '_UNKNOWN') as company_key,
    coalesce(dd.date_key,    '_UNKNOWN') as nav_date_key
  from base b
  left join {{ ref('dim_funds') }}    df on df.fund_name  = b.fund_name
  left join {{ ref('dim_company') }} dc on dc.company_id = b.company_id
  left join {{ ref('dim_date') }}    dd on dd.date       = b.nav_date
),

-- Current flag: latest nav_date per fund+company
with_current as (
  select
    l.*,
    case
      when l.nav_date = max(l.nav_date) over (partition by l.fund_name, l.company_id) then true
      else false
    end as is_current_company
  from lkp l
)

select
  fund_key,
  company_key,
  nav_date_key,
  -- keep naturals for audit if you want
  fund_name,
  company_id,
  company_name,
  nav_date,
  scale_to_fund,
  valuation_amount,
  company_nav,
  is_current_company
from with_current
order by fund_name, company_name, nav_date