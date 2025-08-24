with src as (
  select
    trim(upper(fund_name))                                   as fund_name,
    max(try_to_number(fund_size))                            as fund_size_latest,   -- latest, not historical
    max(trim(sector))                                        as sector,
    max(trim(country))                                       as country,
    max(trim(region))                                        as region
  from {{ ref('stg_fund_data') }}
  where fund_name is not null and trim(fund_name) <> ''
  group by 1
),

regular as (
  select
    {{ dbt_utils.generate_surrogate_key(['fund_name']) }}    as fund_key,
    fund_name,
    fund_size_latest,
    sector, country, region
  from src
),

unknown as (
  select
    '_UNKNOWN'              as fund_key,    -- sentinel for unmatched/late data
    'UNKNOWN'               as fund_name,
    cast(null as number)    as fund_size_latest,
    cast(null as string)    as sector,
    cast(null as string)    as country,
    cast(null as string)    as region
),

final as (
  select * from unknown
  union all
  select * from regular
)

select *
from final
order by case when fund_key = '_UNKNOWN' then 0 else 1 end, fund_name