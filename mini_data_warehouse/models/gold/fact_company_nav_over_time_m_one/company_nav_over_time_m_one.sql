with cov as (
    select
        fund_name,
        company_id,
        company_name,
        valuation_date,
        valuation_amount
    from {{ ref('company_valuations_v') }}
),
own as (
    select
        fund_name,
        asof_date,
        ownership_pct
    from {{ ref('fund_ownership_v') }}
)

select
    cov.fund_name,
    cov.company_id,
    cov.company_name,
    cov.valuation_date as nav_date,
    own.ownership_pct,
    cov.valuation_amount,
    cov.valuation_amount * own.ownership_pct as company_nav
from cov
join own
  on own.fund_name = cov.fund_name
 and own.asof_date = cov.valuation_date
order by cov.fund_name, cov.company_name, nav_date