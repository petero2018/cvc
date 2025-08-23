select
  fund_name,
  transaction_date as event_date,
  transaction_type,
  transaction_amount,
  fund_size_latest
from {{ ref('stg_fund_data') }};
