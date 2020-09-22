with
ms as (
select 
    * 
  from 
    `datamart-finance.datasource_workday.master_b2b_corporate`
)
, fact as (
select
  distinct
  workday_business_id as business_id
  , company_name
from
  `prod-datarangers.galaxy_stg.corporate_account` 
where
  workday_business_id is not null
)

select 
  fact.*
  , date(current_timestamp(), 'Asia/Jakarta') as processed_date 
from 
  fact 
  left join ms on fact.business_id = ms.business_id 
where 
  ms.business_id is null