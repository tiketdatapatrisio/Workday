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
    and payment_timestamp < (select filter3 from fd) 
  group by
    order_id
    , payment_timestamp
)
, ocd as (
  select
    order_id
    , order_detail_id
    , order_type
    , order_name_detail
  from
    `datamart-finance.staging.v_order__cart_detail`
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp < (select filter3 from fd)
    and order_type in ('event','car','tixhotel')
    and order_detail_status in ('active','refund','refunded','hide_by_cust')
  group by
    1,2,3,4
)
, decm as (
  select
    detail_id as detail_event_id
    , string_agg(distinct supplier_id) as supplier_id
    , string_agg(distinct supplier_name) as supplier_name
    , string_agg(distinct event_name) as event_name --Update by Rizki Habibie @2020, 18th of August
    , string_agg(distinct event_type) as event_type
    , string_agg(distinct ext_source) as ext_source_event
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
    , supplier_id
    , case 
        when event_category = 'HOTEL' and lower(supplier_name) not like 'ttd %' then concat('TTD ', supplier_name)
        else supplier_name
      end as supplier_name
    , ext_source_event
    , case
        when event_category = 'HOTEL' then 'Hotel'
        when lower(event_name) LIKE'%sewa mobil%' AND event_category='TRANSPORT' then 'Car' --Update by Rizki Habibie @2020, 18th of August
        when event_type in ('D') then 'Attraction'
        when event_type in ('E') then 'Activity'
        when event_type not in ('D','E') then 'Event'
      end as product_category
  from
    `datamart-finance.staging.v_order__event_connect_ms`
    left join decm using (detail_event_id)
  group by
    1,2,3,4,5,6,7
)
, occar as (
  select
    distinct
    order_detail_id
    , replace(split(split(log_data,'business_id":')[safe_offset(1)],',"')[safe_offset(0)],'"','') as supplier_id
    , replace(split(split(log_data,'business_name":')[safe_offset(1)],',"')[safe_offset(0)],'"','') as supplier_name
    , 'Car' as product_category
  from
    `datamart-finance.staging.v_order__cart_car`
  where
    lastupdate >= (select filter2 from fd)
    and lastupdate < (select filter3 from fd)
)
, hb as (
  select
    safe_cast(id as string) as hotel_itinerarynumber
    , hotel_id as hotel_id_hb
  from
    `datamart-finance.staging.v_hotel_bookings`
)
, hcc as (
  select
    distinct
    _id as city_id
    , string_agg(distinct coalesce(cityName_name, cityName_nameAlias)) as city_name
  from
    `datamart-finance.staging.v_hotel_core_city_flat` 
  where
    cityName_lang = 'en'
    and name_lang = 'en'
  group by
    _id
)
, hcct as (
  select
    distinct
    _id as country_id
    , string_agg(distinct name_name) as country_name
  from
    `datamart-finance.staging.v_hotel_core_country_flat` 
  where
    name_lang = 'en'
    and countryName_lang = 'en'
  group by
    _id
)
, banks as (
  select
    id as bank_id
    , string_agg(distinct name) as bank_name
  from
    `datamart-finance.staging.v_banks`
  group by
    1
)
, hpi as (
  select
    distinct
    hotel_id as id
    , bank_name
    , bank_branch
    , account_number
    , account_holder_name
    , swift_code
  from
    `datamart-finance.staging.v_hotel_payment_informations` 
    left join banks using (bank_id)
)
, hpt as (
  select
    distinct
    business_id as hotel_id
    , type
  from
    `datamart-finance.staging.v_hotel__payment_type` 
  where
    status = 'active'
)
, htls as (
  select
    id as hotel_id_hb
    , string_agg(distinct coalesce(name,alias)) as hotel_name_hb
    , string_agg(distinct address) as hotel_address_hb
    , string_agg(distinct city_name) as hotel_city_hb
    , string_agg(distinct country_name) as hotel_country_hb
    , string_agg(distinct bank_name) as hotel_bank_name
    , string_agg(distinct bank_branch) as hotel_bank_branch
    , string_agg(distinct account_number) as hotel_account_number
    , string_agg(distinct account_holder_name) as hotel_account_holder_name
    , string_agg(distinct swift_code) as hotel_swift_code
  from
    `datamart-finance.staging.v_hotels`
    left join hcc using (city_id)
    left join hcct using (country_id)
    left join hpi using (id)
  where
    active_status >= 0
  group by
    1
)
, oth as (
  select
    order_id
    , string_agg(distinct safe_cast(hb.hotel_id_hb as string)) as supplier_id
    , string_agg(distinct htls.hotel_name_hb) as supplier_name
    , string_agg(distinct htls.hotel_address_hb) as address_name
    , string_agg(distinct htls.hotel_city_hb) as city_name
    , string_agg(distinct htls.hotel_country_hb) as country_name
    , string_agg(distinct room_source) as room_source
    , string_agg(distinct hotel_bank_name) as hotel_bank_name
    , string_agg(distinct hotel_bank_branch) as hotel_bank_branch
    , string_agg(distinct hotel_account_number) as hotel_account_number
    , string_agg(distinct hotel_account_holder_name) as hotel_account_holder_name
    , string_agg(distinct hotel_swift_code) as hotel_swift_code
    , 'Hotel' as product_category
  from
    `datamart-finance.staging.v_order__tixhotel` oth
    left join hb using (hotel_itinerarynumber)
    left join htls using (hotel_id_hb)
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp < (select filter3 from fd)
    and room_source = 'TIKET'
  group by
    1
)
, combine as (
  select
    distinct
    coalesce(oecm.supplier_id, occar.supplier_id, oth.supplier_id) as supplier_id
    , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(trim(coalesce(oecm.supplier_name, occar.supplier_name, oth.supplier_name))), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
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
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(trim(coalesce(oecm.supplier_name, occar.supplier_name, oth.supplier_name)), 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
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
        trim(coalesce(oecm.supplier_name, occar.supplier_name, oth.supplier_name))
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†','') AS supplier_name
    , coalesce(oecm.product_category, occar.product_category, oth.product_category) as product_category
    , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(city_name), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
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
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(city_name, 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
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
        (city_name)
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†','') AS city_name
    , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(country_name), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
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
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(country_name, 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
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
        (country_name)
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†','') AS country_name
    , room_source
    , coalesce(address_name,'-') as address_name
    , hotel_bank_name
    , hotel_bank_branch
    , hotel_account_number
    , hotel_account_holder_name
    , hotel_swift_code
    , case 
        when string_agg(distinct ocd.order_type) = 'car' then 
          case 
            when string_agg(distinct order_name_detail) like '%EXTRA%' or string_agg(distinct order_name_detail) like '%BEST PRICE%' then 'Deposit Deduction'
            else 'TT'
          end
        when string_agg(distinct ocd.order_type) = 'event' then
          case
            when string_agg(distinct ext_source_event) = 'BE_MY_GUEST' then 'Deposit Deduction'
            else 'TT'
          end 
        when string_agg(distinct ocd.order_type) = 'tixhotel' then 
          case
            when string_agg(distinct hpt.type) = 'deposit' then 'Deposit Deduction'
            when string_agg(distinct hpt.type) = 'creditcard' then 'Credit Card'
            else 'TT'
          end
      end as payment_type
    , max(payment_datetime) as max_payment_datetime
  from
    oc
    inner join ocd using (order_id)
    left join oth using (order_id)
    left join oecm using (order_detail_id)
    left join occar using (order_detail_id)
    left join hpt on safe_cast(hpt.hotel_id as string) = oth.supplier_id
  where
    coalesce(oecm.product_category, occar.product_category, oth.product_category) is not null
  group by 1,2,3,4,5,6,7,8,9,10,11,12
)
, add_row_number as (
  select
    *
    , row_number() over(partition by supplier_id order by max_payment_datetime desc) as rn
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
      supplier_id is not null
      and length(supplier_id) > 0
      and supplier_id != '-'
      and supplier_id != '0'
    )
)
, ms as (
  select 
    * 
  from 
    `datamart-finance.datasource_workday.master_supplier`
)
select 
  fact.*
  , date(current_timestamp(),'Asia/Jakarta') as processed_date 
from 
  fact 
  left join ms on fact.supplier_id = ms.supplier_id 
where 
  ms.supplier_id is null