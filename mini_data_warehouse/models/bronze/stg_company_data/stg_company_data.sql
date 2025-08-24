select
  trim(fund_name::string)                          as fund_name,
  trim(company_id::string)                         as company_id,
  trim(company_name::string)                       as company_name,
  trim(upper(transaction_type::string))            as transaction_type,
  try_to_number(transaction_index)                 as transaction_index,
  try_to_date(transaction_date, 'DD/MM/YYYY')      as transaction_date,
  CAST(transaction_amount AS DECIMAL(18,2))        as transaction_amount,
  trim(sector::string)                             as sector,
  trim(country::string)                            as country,
  trim(region::string)                             as region
from {{ source('seeds', 'company_data') }}


