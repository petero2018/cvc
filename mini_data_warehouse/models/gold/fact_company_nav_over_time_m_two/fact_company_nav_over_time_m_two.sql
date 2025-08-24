with cov as (
    select
        fund_name,
        company_id,
        company_name,
        valuation_date,
        valuation_amount
    from {{ ref('company_valuations_v') }}
),
fund_nav as (
    select
        fund_name,
        nav_date,
        nav
    from {{ ref('fact_fund_nav_over_time') }}
),
totals as (
    select
        fund_name,
        valuation_date,
        sum(valuation_amount) as total_company_val
    from cov
    group by 1,2
),
scale as (
    select
        t.fund_name,
        t.valuation_date,
        case
            when t.total_company_val = 0 then null
            else f.nav / t.total_company_val
        end as scale_to_fund
    from totals t
    join fund_nav f
      on f.fund_name = t.fund_name
     and f.nav_date  = t.valuation_date
)

select
    c.fund_name,
    c.company_id,
    c.company_name,
    c.valuation_date as nav_date,
    s.scale_to_fund,
    c.valuation_amount,
    c.valuation_amount * s.scale_to_fund as company_nav
from cov c
join scale s
  on s.fund_name      = c.fund_name
 and s.valuation_date = c.valuation_date
order by c.fund_name, c.company_name, nav_date