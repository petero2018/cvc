with ranked as (
  select
    fund_name                                          as fund_name,    
    company_id                                         as company_id,               
    company_name                                       as company_name,
    transaction_date                                   as valuation_date,
    transaction_amount                                 as valuation_amount,
    row_number() over (
      partition by fund_name, company_id, transaction_date
      -- choose ONE of the two ORDER BY lines:
      order by coalesce(transaction_index, 9e18)  -- use index if present (NULLs last)
      -- order by valuation_amount desc           -- or: keep the highest value that day
    ) as rn
  from {{ ref('stg_company_data') }}
  where transaction_type = 'VALUATION'
)
select
  fund_name,
  company_id,
  company_name,
  valuation_date,
  valuation_amount
from ranked
qualify rn = 1


