select
  trim(fund_name)                               as fund_name,
  try_to_date(transaction_date, 'DD/MM/YYYY')   as event_date,
  upper(trim(transaction_type))                 as transaction_type,
  try_to_number(transaction_amount)             as transaction_amount,
  try_to_number(fund_size)                      as fund_size_latest,
  try_to_number(transaction_index)              as transaction_index,
  trim(sector)                                  as sector,
  trim(country)                                 as country,
  trim(region)                                  as region
from {{ ref('stg_fund_data') }}