with src as (
  select
    trim(upper(company_id))   as company_id,
    max(trim(company_name))   as company_name,
    max(trim(sector))         as sector,
    max(trim(country))        as country,
    max(trim(region))         as region
  from {{ ref('stg_company_data') }}
  where company_id is not null and trim(company_id) <> ''
  group by 1
),

regular as (
  select
    {{ dbt_utils.generate_surrogate_key(['company_id']) }} as company_key,
    company_id,
    company_name,
    sector, country, region
  from src
),

unknown as (
  select
    '_UNKNOWN'            as company_key,     -- sentinel key
    'UNKNOWN'             as company_id,
    'UNKNOWN'             as company_name,
    cast(null as string)  as sector,
    cast(null as string)  as country,
    cast(null as string)  as region
),

final as (
  select * from unknown
  union all
  select * from regular
)

select *
from final
order by case when company_key = '_UNKNOWN' then 0 else 1 end, company_id