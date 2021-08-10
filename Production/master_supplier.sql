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
, evoo as ( /*new datasource event/TTD @7 Jan 2021*/
  select
   * except (product_subcategory,ps)
   , string_agg(distinct lower(trim(json_extract_scalar(ps,'$.code')))) as product_subcategory
   , case
        when product_primary_category in ('attraction','playground') then 'Attraction'
        when product_primary_category in ('beauty_wellness','class_workshop','culinary','food_drink','game_hobby','tour','travel_essential') then 'Activity'
        when product_primary_category = 'event' then 'Event'
        when product_primary_category = 'transport' and supplier_name = 'Railink' then 'Train' 
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
      end as supplier_category
    from
    (
      select
        * except (product_subcategories, rn)
        , json_extract_array(product_subcategories,'$.product_subcategories') as product_subcategory
      from (
        select
          safe_cast(coreorderid as int64) as order_id
          , case
              when trim(product_supplierCode) = 'S2' then '23196226'
              when trim(product_supplierCode) = 'S3' then '33505623'
              when trim(product_supplierCode) = 'S6' then '33505604'
              else json_extract_scalar (product_productPartners, '$.product_productPartners[0].businessId')
            end as supplier_id
         , case
            when trim(product_supplierCode) = 'S2' then 'bemyguest'
            when trim(product_supplierCode) = 'S3' then 'eazyspadeals'
            when trim(product_supplierCode) = 'S6' then 'klook'
            else json_extract_scalar (product_productPartners, '$.product_productPartners[0].name')
          end as supplier_name
          , lower(trim(product_primaryCategory)) as product_primary_category
          , product_subcategories
          , lower(disbursement_type) as disbursement_type
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
      product_subcategories,supplier_id,1,2,3,4,5
)
left join unnest(product_subcategory) as ps
  group by
    1,2,3,4,5
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
, hcr as (
  select
    distinct
    _id as region_id
    , string_agg(distinct coalesce(regionName_name, name_name)) as region_name
    , string_agg(distinct coalesce(rc.region_code,'')) as region_code
  from
    `datamart-finance.staging.v_hotel_core_region_flat` rf
  left join `datamart-finance.staging.v_workday_mapping_region_code` rc
    on rf._id = rc.region_id
  where
    regionName_lang = 'en'
    and name_lang = 'en'
  group by
    _id
)
, hcct as (
  select
    distinct
    _id as country_id
    , string_agg(distinct replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(name_name), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
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
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(name_name, 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
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
        (name_name)
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†','')) as country_name
    , string_agg(distinct coalesce(cd.country_code,'')) as country_code
  from
    `datamart-finance.staging.v_hotel_core_country_flat` cf
  left join `datamart-finance.staging.v_workday_mapping_country_code` cd
    on cf._id = cd.country_id
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
    , string_agg(distinct region_code) as hotel_region_hb
    , string_agg(distinct country_code) as hotel_country_hb
    , string_agg(distinct bank_name) as hotel_bank_name
    , string_agg(distinct bank_branch) as hotel_bank_branch
    , string_agg(distinct account_number) as hotel_account_number
    , string_agg(distinct account_holder_name) as hotel_account_holder_name
    , string_agg(distinct swift_code) as hotel_swift_code
    , string_agg(distinct postal_code) as hotel_postal_code
  from
    `datamart-finance.staging.v_hotels`
    left join hcc using (city_id)
    left join hcr using (region_id)
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
    , string_agg(distinct htls.hotel_region_hb) as region_code
    , string_agg(distinct htls.hotel_country_hb) as country_code
    , string_agg(distinct room_source) as room_source
    , string_agg(distinct hotel_bank_name) as Supplier_Bank_Name
    , string_agg(distinct hotel_bank_branch) as Supplier_Bank_Branch_Name
    , string_agg(distinct hotel_account_number) as Supplier_Bank_Account_Number
    , string_agg(distinct hotel_account_holder_name) as Supplier_Bank_Account_Name
    , string_agg(distinct hotel_swift_code) as Supplier_Bank_BIC_SWIFT_Code
    , string_agg(distinct hotel_postal_code) as Address_Postal_Code
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
    coalesce(evoo.supplier_id, occar.supplier_id, oth.supplier_id) as Supplier_Reference_ID
    , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(trim(coalesce(evoo.supplier_name, occar.supplier_name, oth.supplier_name))), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
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
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(trim(coalesce(evoo.supplier_name, occar.supplier_name, oth.supplier_name)), 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
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
        trim(coalesce(evoo.supplier_name, occar.supplier_name, oth.supplier_name))
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†','') AS Supplier_Name
    , coalesce(evoo.supplier_category, occar.product_category, oth.product_category) as Supplier_Category_ID
    , '' Supplier_Group_ID
    , '' Worktag_Product_Provider_Org_Ref_ID
    , coalesce(evoo.supplier_category, occar.product_category, oth.product_category) as Worktag_Product_Category_Ref_ID
    , '' Supplier_Default_Currency
    , 'Immediate' Payment_Terms
    , 'Deposit_Deduction' Accepted_Payment_Types_1
    , 'Credit_Card' Accepted_Payment_Types_2
    , 'PG_In_Transit' Accepted_Payment_Types_3
    , 'TT' Accepted_Payment_Types_4
    , '' Accepted_Payment_Types_5
    , case 
        when string_agg(distinct ocd.order_type) = 'car' then 
          case 
            when string_agg(distinct order_name_detail) like '%EXTRA%' or string_agg(distinct order_name_detail) like '%BEST PRICE%' then 'Deposit_Deduction'
            else 'TT'
          end
        when string_agg(distinct ocd.order_type) = 'event' then
          case
            when string_agg(distinct evoo.disbursement_type) = 'bank_transfer' then 'TT'
            else 'Deposit_Deduction'
          end 
        when string_agg(distinct ocd.order_type) = 'tixhotel' then 
          case
            when string_agg(distinct hpt.type) = 'deposit' then 'Deposit_Deduction'
            when string_agg(distinct hpt.type) = 'creditcard' then 'Credit_Card'
            else 'TT'
          end
      end as Default_Payment_Type
    , '' Tax_Default_Tax_Code_ID
    , '' Tax_Default_Withholding_Tax_Code_ID
    , '' Tax_ID_NPWP
    , '' Tax_ID_Type
    , '' Transaction_Tax_YN
    , '' Primary_Tax_YN
    , case
        when length(trim(oth.country_code)) > 1 then format_date("%Y-%m-%d", date(payment_datetime))
        else ''
      end as Address_Effective_Date
    , ifnull(oth.country_code,'') as Address_Country_Code
    , coalesce(REPLACE(address_name,'\n',''),'') as Address_Line_1
    , '' Address_Line_2
    , '' Address_City_Subdivision_2
    , '' Address_City_Subdivision_1
    , case
        when ac.address_city is not null and ac.address_city = 'yes'
        then ifnull(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
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
          END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†',''), '')
          when ac.address_city is not null and ac.address_city = 'no' then ''
          when ac.address_city is null then ifnull(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
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
          END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†',''), '')
        else ''
      end AS Address_City
    , case
        when ac.address_region_subdivision_2 is not null and ac.address_region_subdivision_2 = 'yes'
        then ifnull(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
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
          END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†',''), '')
        else ''
      end AS Address_Region_Subdivision_2
    , case
        when ac.address_region_subdivision_1 is not null and ac.address_region_subdivision_1 = 'yes'
          then ifnull(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
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
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†',''), '')
        else ''
      end as Address_Region_Subdivision_1
    , ifnull(region_code,'') as Address_Region_Code
    , ifnull(oth.Address_Postal_Code, '') Address_Postal_Code
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then (ifnull(oth.country_code,''))
        else ''
      end as Supplier_Bank_Country
    , '' Supplier_Bank_Currency
    , '' Supplier_Bank_Account_Nickname
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then 'SA'
        else '' 
      end as Supplier_Bank_Account_Type
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then ifnull(Supplier_Bank_Name,'')
        else ''
      end as Supplier_Bank_Name
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then 'XXX'
        else ''
      end as Supplier_Bank_ID_Routing_Number
    , '' Supplier_Bank_Branch_ID
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then Supplier_Bank_Branch_Name
        else ''
      end as Supplier_Bank_Branch_Name
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then Supplier_Bank_Account_Number
        else ''
      end as Supplier_Bank_Account_Number
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then Supplier_Bank_Account_Name
        else ''
      end as Supplier_Bank_Account_Name
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 and REGEXP_CONTAINS(Supplier_Bank_BIC_SWIFT_Code, r'^[a-zA-Z]+$') then ifnull(upper(Supplier_Bank_BIC_SWIFT_Code), '')
        else ''
      end as Supplier_Bank_BIC_SWIFT_Code
    , max(payment_datetime) as max_payment_datetime
  from
    oc
    inner join ocd using (order_id)
    left join oth using (order_id)
    left join evoo using (order_id)
    /*left join oecm using (order_detail_id)
    left join decm using (detail_event_id)*/
    left join occar using (order_detail_id)
    left join hpt on safe_cast(hpt.hotel_id as string) = oth.supplier_id
    left join `datamart-finance.staging.v_workday_mapping_address_components` ac
      on oth.country_code = ac.country_code
  where
    coalesce(evoo.supplier_category, occar.product_category, oth.product_category) is not null
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42
)
, add_row_number as (
  select
    *
    , row_number() over(partition by Supplier_Reference_ID order by max_payment_datetime desc) as rn
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
      Supplier_Reference_ID is not null
      and length(Supplier_Reference_ID) > 0
      and Supplier_Reference_ID != '-'
      and Supplier_Reference_ID != '0'
    )
)
, ms as (
  select 
    * 
  from 
    `datamart-finance.datasource_workday.master_data_supplier`
)

select 
  fact.*
  , date(current_timestamp(),'Asia/Jakarta') as processed_date 
from 
  fact 
  left join ms on fact.Supplier_Reference_ID = ms.Supplier_Reference_ID 
where 
  ms.Supplier_Reference_ID is null