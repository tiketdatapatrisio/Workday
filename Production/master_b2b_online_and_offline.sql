with
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -1 day) as filter2
     ,timestamp_add(filter1, interval 3 day) as filter3 
  from
  (
    select
     timestamp_add(timestamp(date(current_timestamp(), 'Asia/Jakarta')), interval -79 hour) as filter1
  )
)
, oc as (
  select
    distinct
    case
      when reseller_type in ('tiket_agent','txtravel','agent','affiliate') then reseller_id
      when reseller_type in ('reseller','widget') then reseller_id
      else null
    end as business_id
    , case
        when reseller_type in ('tiket_agent','txtravel','agent','affiliate') then 'B2B Offline'
        when reseller_type in ('reseller','widget') then 'B2B Online'
        else null
      end as customer_type
  from
    `prod-datarangers.galaxy_stg.order__cart`
  where
    payment_status = 'paid'
    and payment_timestamp >= (select filter1 from fd)
    and payment_timestamp < (select filter3 from fd)
)
, bp as (
  select
    business_id
    , business_name
  from
    `prod-datarangers.galaxy_stg.business__profile` 
)
, fact as (
select
  business_id
  , business_name
  , customer_type
from
  oc
  left join bp using (business_id)
where
  business_id is not null
)
, ms as (
  select 
    * 
  from 
    `datamart-finance.datasource_workday.master_b2b_online_and_offline`
)

select 
  fact.*
  , date(current_timestamp(), 'Asia/Jakarta') as processed_date 
from 
  fact 
  left join ms on fact.business_id = ms.business_id 
where 
  ms.business_id is null
