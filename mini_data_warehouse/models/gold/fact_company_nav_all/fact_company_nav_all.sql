-- 1) Base: company valuation + ownership on the same date (Method 1 inputs)
with base as (
  select
    trim(upper(v.fund_name))   as fund_name,
    trim(upper(v.company_id))  as company_id,
    v.company_name,
    v.valuation_date           as nav_date,
    v.valuation_amount,
    o.ownership_pct
  from {{ ref('company_valuations_v') }} v
  join {{ ref('fund_ownership_v') }} o
    on o.fund_name = v.fund_name
   and o.asof_date = v.valuation_date
),

-- 2) Method 1 result
m1 as (
  select
    b.*,
    CAST((b.valuation_amount * b.ownership_pct)  AS DECIMAL(18,2)) as company_nav_m1
  from base b
),

-- 3) Bring in fund NAV for Method 2 scaling
fund_nav as (
  select
    trim(upper(fund_name)) as fund_name,
    nav_date,
    nav
  from {{ ref('fact_fund_nav_over_time') }}
),

-- 4) Total company valuation per fund/date (for scaling)
totals as (
  select
    fund_name,
    nav_date,
    sum(valuation_amount) as total_company_val
  from m1
  group by 1,2
),

-- 5) Scale factor so SUM(company_nav_m2) == fund NAV per fund/date
scale as (
  select
    t.fund_name,
    t.nav_date,
    case when t.total_company_val = 0 then null
         else f.nav / t.total_company_val end as scale_to_fund
  from totals t
  join fund_nav f
    on f.fund_name = t.fund_name
   and f.nav_date  = t.nav_date
),

-- 6) Method 2 result
m2 as (
  select
    m1.*,
    s.scale_to_fund,
    CAST(m1.valuation_amount * s.scale_to_fund AS DECIMAL(18,2)) as company_nav_m2
  from m1
  left join scale s
    on s.fund_name = m1.fund_name
   and s.nav_date  = m1.nav_date
),

-- 7) Dimension key lookups (no coalesce here; let tests/watchers catch nulls)
lkp as (
  select
    m2.*,
    df.fund_key,
    dc.company_key,
    dd.date_key as nav_date_key
  from m2
  left join {{ ref('dim_funds') }}    df on df.fund_name  = m2.fund_name
  left join {{ ref('dim_company') }}  dc on dc.company_id = m2.company_id
  left join {{ ref('dim_date') }}     dd on dd.date       = m2.nav_date
)

-- 8) Final output + current flag
select
  -- surrogate keys
  fund_key,
  company_key,
  nav_date_key,

  -- naturals (for audit / human-friendly filters)
  fund_name,
  company_id,
  company_name,
  nav_date,

  -- inputs + outputs
  ownership_pct,
  valuation_amount,
  company_nav_m1,         -- Method 1: ownership × valuation
  scale_to_fund,          -- factor so Σ(company_nav_m2) = fund NAV
  company_nav_m2,         -- Method 2: scaled to fund NAV

  -- quality/monitoring helpers
  case when nav_date = max(nav_date) over (partition by fund_name, company_id)
       then true else false end as is_current_company
from lkp
order by fund_name, company_name, nav_date
