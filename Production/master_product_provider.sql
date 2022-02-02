with
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -1 day) as filter2
    , timestamp_add(filter1, interval 3 day) as filter3 
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
/*, decm as ( --moved to ms (eventorder) / 7 Jan 2021
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
       /* else product_provider_name
      end as product_provider_name
    , case
        when event_category = 'HOTEL' then 'Hotel'
        when lower(product_provider_name) like ('%sewa mobil%') and event_category='TRANSPORT' then 'Car' /* TTD car */
       /* when event_type in ('D') then 'Attraction'
        when event_type in ('E') then 'Activity'
        when event_type not in ('D','E') then 'Event'
      end as product_category
  from
    `datamart-finance.staging.v_order__event_connect_ms`
  left join decm using (detail_event_id)
  group by
    1,2,3,4,5,6
)*/
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
    `datamart-finance.staging.v_flight__airlines` 
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
, ot as (
  select
    order_id
    , coalesce(itinerary_id, hotel_itinerarynumber) as hotel_itinerarynumber
  from
    `datamart-finance.staging.v_order__tixhotel`
  left join
    (
      select
        safe_cast(OrderId as int64) as order_id
        , itineraryId as itinerary_id
      from
        `datamart-finance.staging.v_hotel_cart_book`
      where
        createdDate >= (select filter2 from fd)
        and lower(status) not in ('canceled','pending')
    )
    using(order_id)
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp < (select filter3 from fd)
    and room_source = 'TIKET'
)
, oth as (
  select
    order_id
    , string_agg(distinct safe_cast(hb.hotel_id_hb as string)) as product_provider_id
    , string_agg(distinct htls.hotel_name_hb) as product_provider_name
    , 'Hotel' as product_category
  from
    ot
    left join hb using (hotel_itinerarynumber)
    left join htls using (hotel_id_hb)
  group by
    1
)
, evoo as ( /* new datasource event/TTD @7 Jan 2021*/
  select
   * except (product_subcategory,ps)
   , string_agg(distinct lower(trim(json_extract_scalar(ps,'$.code')))) as product_subcategory
   , case
        when product_primary_category in ('attraction','playground','sport_outdoor') then 'Attraction'
        when product_primary_category in ('beauty_wellness','class_workshop','culinary','food_drink','game_hobby','tour','travel_essential','covid19_test') then 'Activity'
        when product_primary_category = 'event' then 'Event'
        when product_primary_category = 'transport' and product_supplier = 'Railink' then 'Train' 
        when product_primary_category = 'transport'
          and lower(product_name) like '%rental%sewa motor%'
          then 'Car'
        when product_primary_category = 'transport' 
        and 
          ( string_agg(distinct lower(trim(json_extract_scalar(ps,'$.code')))) like '%airport%'
            or string_agg(distinct lower(trim(json_extract_scalar(ps,'$.code')))) like'%lepas kunci%' 
            or string_agg(distinct lower(trim(json_extract_scalar(ps,'$.code')))) like'%city to city%'
          )  
        then 'Car' 
        when product_primary_category = 'transport' then 'Activity'
        when product_primary_category = 'hotel' then 'Hotel'
        else product_primary_category
      end as product_category
    , case
        when product_primary_category = 'hotel' then concat(product_name, ' [TTD]')
        else product_name
        end as  product_provider_name
    from
    (
      select
        * except (product_subcategories, rn)
        , json_extract_array(product_subcategories,'$.product_subcategories') as product_subcategory
      from (
        select
          safe_cast(coreorderid as int64) as order_id
          , product__id as product_provider_id
          , lower(trim(product_primaryCategory)) as product_primary_category
          , json_extract_scalar (product_productPartners, '$.product_productPartners[0].name') product_supplier
          , product_subcategories
          , json_extract_scalar (product_translations, '$.product_translations[0].title') as  product_name
          , row_number() over(partition by coreorderId, pt.code order by lastModifiedDate desc) as rn
        from
            `datamart-finance.staging.v_events_v2_order__order_l2`
        left join unnest (priceTierQuantities) as pt
        left join unnest(tickets) as t
        where status like '%ISSUED%'
        and createdDate >= (select filter1 from fd)
        and createdDate <=(select filter3 from fd)
        and lastModifiedDate >= (select filter2 from fd)
          )
   where
      rn = 1
    group by
      product_subcategories,product_provider_id,1,2,3,4,5
)
left join unnest(product_subcategory) as ps
  group by
    1,2,3,4,5
)

, combine as (
  select
    distinct
    coalesce(evoo.product_provider_id, occar.product_provider_id, oth.product_provider_id, safe_cast(ocd.order_master_id as string)) as Organization_Reference_ID 
    , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(trim(coalesce(evoo.product_provider_name, occar.product_provider_name, oth.product_provider_name, ac.airlines_real_name))), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
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
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(trim(coalesce(evoo.product_provider_name, occar.product_provider_name, oth.product_provider_name, ac.airlines_real_name)), 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
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
        trim(coalesce(evoo.product_provider_name, occar.product_provider_name, oth.product_provider_name, ac.airlines_real_name))
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†','') as Product_Provider_Name  /* Change column for the new data template */
    , coalesce(evoo.product_category, occar.product_category, oth.product_category, ac.flight_product_category) as Product_Provider_Hierarchy /* Add column for the new data template */
    , coalesce(evoo.product_category, occar.product_category, oth.product_category, ac.flight_product_category) as Product_Category /* Change column for the new data template */
    , max(payment_datetime) as max_payment_datetime
  from
    oc
    inner join ocd using (order_id)
    left join oth using (order_id)
    left join evoo using (order_id)
   /* left join oecm using (order_detail_id) */
    left join occar using (order_detail_id)
    left join ac using (order_master_id)
  where
    coalesce(evoo.product_category, occar.product_category, oth.product_category, ac.flight_product_category) is not null
  group by 
    1,2,3
)
, add_row_number as (
  select
    *
    , row_number() over(partition by Organization_Reference_ID
    order by max_payment_datetime desc) as rn
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
      Organization_Reference_ID is not null
      and length(Organization_Reference_ID) > 0
      and Organization_Reference_ID != '-'
      and Organization_Reference_ID != '0'
    )
)
, ms as (
  select 
    * 
  from 
    `datamart-finance.datasource_workday.master_data_product_provider`
)

select 
  fact.*
  , date(current_timestamp(), 'Asia/Jakarta') as processed_date 
from 
  fact
  left join ms on fact.Organization_Reference_ID = ms.Organization_Reference_ID 
where
  ms.Organization_Reference_ID is null