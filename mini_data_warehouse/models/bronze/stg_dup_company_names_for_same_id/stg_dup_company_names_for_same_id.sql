-- Fails if any company_id maps to more than one company_name in the raw seed
select
  company_id,
  count(distinct company_name) as name_variants
from {{ source('seeds','company_data') }}
group by 1
having count(distinct company_name) > 1
