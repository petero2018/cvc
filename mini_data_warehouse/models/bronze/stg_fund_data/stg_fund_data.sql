select
  trim(fund_name::string) as fund_name,
  try_to_date(transaction_date, 'DD/MM/YYYY') as transaction_date,
  trim(upper(transaction_type::string)) as transaction_type,
  to_number(transaction_amount) as transaction_amount,
  to_number(fund_size) as fund_size,
  try_to_number(transaction_index) as transaction_index
from {{ source('seeds', 'fund_data') }}
