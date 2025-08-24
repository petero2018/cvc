with cov as (
  select
    trim(upper(fund_name))   as fund_name,
    trim(upper(company_id))  as company_id,
    company_name,
    valuation_date,
    valuation_amount
  from {{ ref('company_valuations_v') }}
),

own as (
  select
    trim(upper(fund_name))   as fund_name,
    asof_date,
    least(ownership_pct, 1.0) as ownership_pct  -- cap here (optional)
  from {{ ref('fund_ownership_v') }}
),

base as (
  select
    c.fund_name,
    c.company_id,
    c.company_name,
    c.valuation_date as nav_date,
    c.valuation_amount,
    o.ownership_pct,
    CAST(c.valuation_amount * o.ownership_pct AS DECIMAL(18,2)) as company_nav
  from cov c
  join own o
    on o.fund_name = c.fund_name
   and o.asof_date = c.valuation_date
),

lkp as (
  select
    b.*,
    df.fund_key,
    dc.company_key,
    dd.date_key as nav_date_key
  from base b
  left join {{ ref('dim_funds') }}   df on df.fund_name  = b.fund_name
  left join {{ ref('dim_company') }} dc on dc.company_id = b.company_id
  left join {{ ref('dim_date') }}    dd on dd.date       = b.nav_date
),

final as (
  select
    coalesce(fund_key,    '_UNKNOWN') as fund_key,
    coalesce(company_key, '_UNKNOWN') as company_key,
    coalesce(nav_date_key,'_UNKNOWN') as nav_date_key,
    -- naturals for audit
    fund_name, company_id, company_name, nav_date,
    ownership_pct, valuation_amount, company_nav
  from lkp
),

with_current as (
  select
    f.*,
    case when f.nav_date = max(f.nav_date) over (partition by f.fund_name, f.company_id)
         then true else false end as is_current_company
  from final f
)

select * from with_current
order by fund_name, company_name, nav_date