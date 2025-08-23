select
  trim(fund_name) as fund_name,
  try_to_date(transaction_date) as transaction_date,
  upper(transaction_type) as transaction_type,
  try_to_number(transaction_amount) as transaction_amount,
  try_to_number(fund_size) as fund_size_latest,
  transaction_index
from {{ source('raw', 'fund_data') }};
