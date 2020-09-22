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
    order_id
    , datetime(payment_timestamp, 'Asia/Jakarta') as payment_datetime
  from
    `datamart-finance.staging.v_order__cart`
  where
    payment_status = 'paid'
    and payment_timestamp >= (select filter1 from fd)
    and payment_timestamp < (select filter3 from fd) /* */
  group by
    order_id
    , payment_timestamp
)
, ocd as (
  select
    order_id
    , order_detail_id
    , order_master_id
  from
    `datamart-finance.staging.v_order__cart_detail`
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp < (select filter3 from fd) /* */
    and order_type in ('event','car','tixhotel','flight')
    and order_detail_status in ('active','refund','refunded','hide_by_cust')
  group by
    1,2,3
)
, decm as (
  select
    detail_id as detail_event_id
    , string_agg(distinct business_id) as product_provider_id
    , string_agg(distinct event_name) as product_provider_name
    , string_agg(distinct event_type) as event_type
    , string_agg(distinct event_category) as event_category
  from
    `datamart-finance.staging.v_detail__event_connect_ms` 
  group by
    1
)
, oecm as (
  select
    order_id
    , order_detail_id
    , detail_event_id
    , product_provider_id
    , case
        when event_category = 'HOTEL' then concat(product_provider_name, ' [TTD]') /* to mark tiket flexi hotel */
        else product_provider_name
      end as product_provider_name
    , case
        when event_category = 'HOTEL' then 'Hotel'
        when lower(product_provider_name) like ('%sewa mobil%') and event_category='TRANSPORT' then 'Car' --TTD car
        when event_type in ('D') then 'Attraction'
        when event_type in ('E') then 'Activity'
        when event_type not in ('D','E') then 'Event'
      end as product_category
  from
    `datamart-finance.staging.v_order__event_connect_ms`
  left join decm using (detail_event_id)
  group by
    1,2,3,4,5,6
)
, occar as (
  select
    distinct
    order_detail_id
    , replace(split(split(log_data,'business_id":')[safe_offset(1)],',"')[safe_offset(0)],'"','') as product_provider_id
    , replace(split(split(log_data,'business_name":')[safe_offset(1)],',"')[safe_offset(0)],'"','') as product_provider_name
    , 'Car' as product_category
  from
    `datamart-finance.staging.v_order__cart_car`
  where
    lastupdate >= (select filter2 from fd)
    and lastupdate < (select filter3 from fd) /* */
)
, hb as (
  select
    safe_cast(id as string) as hotel_itinerarynumber
    , hotel_id as hotel_id_hb
  from
    `datamart-finance.staging.v_hotel_bookings`
)
, hcr as (
  select
    distinct
    _id as region_id
    , string_agg(distinct regionName_name) as region_name
  from
    `datamart-finance.staging.v_hotel_core_region_flat`
  where
    regionName_lang = 'en'
  group by
    _id
)
, ac as (
  select
    master_id as order_master_id
    , string_agg(distinct airlines_real_name) as airlines_real_name
    , 'Flight' as flight_product_category
  from
    `datamart-finance.staging.v_airlines_code` 
  group by
    1
)
, htls as (
  select
    id as hotel_id_hb
    , string_agg(distinct coalesce(name,alias)) as hotel_name_hb
    , string_agg(distinct region_name) as region_name
  from
    `datamart-finance.staging.v_hotels`
    left join hcr using (region_id)
  where
    active_status >= 0
  group by
    1
)
, oth as (
  select
    order_id
    , string_agg(distinct safe_cast(hb.hotel_id_hb as string)) as product_provider_id
    , string_agg(distinct htls.hotel_name_hb) as product_provider_name
    , 'Hotel' as product_category
  from
    `datamart-finance.staging.v_order__tixhotel` oth
    left join hb using (hotel_itinerarynumber)
    left join htls using (hotel_id_hb)
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp < (select filter3 from fd) /* */
    and room_source = 'TIKET'
  group by
    1
)
, combine as (
  select
    distinct
    coalesce(oecm.product_provider_id, occar.product_provider_id, oth.product_provider_id, safe_cast(ocd.order_master_id as string)) as product_provider_id
    , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(trim(coalesce(oecm.product_provider_name, occar.product_provider_name, oth.product_provider_name, ac.airlines_real_name))), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(trim(coalesce(oecm.product_provider_name, occar.product_provider_name, oth.product_provider_name, ac.airlines_real_name)), 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
                          r"[ùúûü]", 'u'),
                        r"[òóôöø]", 'o'),
                      r"[ìíîï]", 'i'),
                    r"[èéêë]", 'e'),
                  r"[àáâäå]", 'a'),
                r"[ÙÚÛÜ]", 'U'),
              r"[ÒÓÔÖØ]", 'O'),
            r"[ÌÍÎÏ]", 'I'),
          r"[ÈÉÊË]", 'E'),
        r"[ÀÁÂÄÅ]", 'A')
      ELSE
        trim(coalesce(oecm.product_provider_name, occar.product_provider_name, oth.product_provider_name, ac.airlines_real_name))
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†','') as product_provider_name
    , coalesce(oecm.product_category, occar.product_category, oth.product_category, ac.flight_product_category) as product_category
    , max(payment_datetime) as max_payment_datetime
  from
    oc
    inner join ocd using (order_id)
    left join oth using (order_id)
    left join oecm using (order_detail_id)
    left join occar using (order_detail_id)
    left join ac using (order_master_id)
  where
    coalesce(oecm.product_category, occar.product_category, oth.product_category, ac.flight_product_category) is not null
  group by 
    1,2,3
)
, add_row_number as (
  select
    *
    , row_number() over(partition by product_provider_id order by max_payment_datetime desc) as rn
  from
    combine
)
, fact as (
  select
    * except (max_payment_datetime,rn)
  from
    add_row_number
  where
    rn = 1
    and (
      product_provider_id is not null
      and length(product_provider_id) > 0
      and product_provider_id != '-'
      and product_provider_id != '0'
    )
)
, ms as (
  select 
    * 
  from 
    `datamart-finance.datasource_workday.master_product_provider`
)
select 
  fact.*
  , date(current_timestamp(), 'Asia/Jakarta') as processed_date 
from 
  fact 
  left join ms on fact.product_provider_id = ms.product_provider_id 
where 
  ms.product_provider_id is null
