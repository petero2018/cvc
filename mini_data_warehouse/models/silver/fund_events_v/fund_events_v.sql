-- models/intermediate/fund_events_v.sql  (materialized: view)
select
  fund_name,
  try_to_date(transaction_date, 'DD/MM/YYYY') as event_date,
  upper(trim(transaction_type))               as transaction_type,
  try_to_number(transaction_amount)          as transaction_amount,
  try_to_number(fund_size_latest)            as fund_size_latest,
  try_to_number(transaction_index)           as transaction_index
from {{ ref('stg_fund_data') }};
