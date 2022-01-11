with
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -1 day) as filter2
    , timestamp_add(filter1, interval 3 day) as filter3
  from
  (
    select
       timestamp_add(timestamp(date(current_timestamp(),'Asia/Jakarta')), interval -79 hour) as filter1
  )
)
, master_event_supplier as (
  select
    *
  from
    `datamart-finance.staging.v_workday_mapping_event_supplier` 
)
, master_event_product_provider as (
  select
    *
  from
    `datamart-finance.staging.v_workday_mapping_event_product_provider`  
)
, master_event_name_deposit as (
  select
    event_name
    , 'Deposit' as deposit_flag
    , start_date
    , coalesce(end_date, '2100-12-31') as end_date
  from
    `datamart-finance.staging.v_workday_mapping_deposit_event` 
  where is_deposit_flag = true
)
, master_event_supplier_deposit as (
  select
    supplier_id
    , workday_supplier_reference_id
    , is_deposit_flag as is_event_supplier_deposit_flag 
  from
    `datamart-finance.staging.v_workday_mapping_supplier` 
  where lower(supplier_category) = 'event'
)
, master_supplier_airport_transfer_and_lounge as (
  select 
    workday_supplier_reference_id
    , workday_supplier_name
    , is_deposit_flag
  from 
    `datamart-finance.staging.v_workday_mapping_supplier`
  where 
    workday_supplier_name in ('Airport Transfer','Tix-Sport Airport Lounge')
  group by 
    1,2,3
)
, ca as (
  select
    distinct 
    lower(trim(EmailAccessB2C)) as account_username
    , workday_business_id as business_id
    , 'corporate' as corporate_flag
  from 
    `datamart-finance.staging.v_corporate_account`
  where
    workday_business_id is not null
    -- and Start_Date <='2020-03-31' -- is not null /* 30 Maret 2021, temporary solution for b2b corp group*/
    and safe_cast(Delete_Flag as date) is null /*add filter when generate CI > 1 April 2021*/
)
, corp as (
  select
    distinct
    email as account_username
    , business_id
    , 'corporate' as corporate_flag
  from
    `datamart-finance.staging.v_tix_affiliate_platform__employees` e
  join (
    select
      id
      , unique_id as business_id
      , row_number() over(partition by id order by updated_at desc) as rn
    from 
      `datamart-finance.staging.v_tix_affiliate_platform__corporates`
  ) c on e.corporate_id = c.id and rn = 1
  where
    e.is_active = 1
    and e.is_deleted = 0
)
, ma_ori as (
  select
    distinct
    account_id
    , lower(replace(replace(account_username,'"',''),'\\','')) as account_username
    , account_last_login as accountlastlogin
    , processed_dttm
  from
    `datamart-finance.staging.v_member__account`
  union distinct
  select
    distinct
    accountid as account_id
    , lower(replace(replace(accountusername,'"',''),'\\','')) as account_username
    , accountlastlogin
    , processed_dttm
  from
    `datamart-finance.staging.v_members_account_admin` 
  union distinct
  select
    distinct
    accountid as account_id
    , lower(replace(replace(accountusername,'"',''),'\\','')) as account_username
    , accountlastlogin
    , processed_dttm
  from
    `datamart-finance.staging.v_members_account_b2c` 
  union distinct
  select
    distinct
    accountid as account_id
    , lower(replace(replace(accountusername,'"',''),'\\','')) as account_username
    , accountlastlogin
    , processed_dttm
  from
    `datamart-finance.staging.v_members_account_b2b` 
)
, ma as (
  select 
    * 
  from (
    select
      *
      , row_number() over(partition by account_id order by processed_dttm desc, accountlastlogin desc) as rn
    from
      ma_ori
  ) 
  where rn = 1
)
, oc as (
  select
    distinct
    order_id
    , account_id
    , account_username
    , datetime(payment_timestamp,'Asia/Jakarta') as payment_timestamp_oc
    , reseller_type
    , coalesce(ca.corporate_flag, corp.corporate_flag) as corporate_flag
    , coalesce(ca.business_id, corp.business_id) as business_id
    , reseller_id
  from
    `datamart-finance.staging.v_order__cart`
    left join ma using (account_id)
    left join ca using (account_username)
    left join corp using (account_username)
  where
    payment_timestamp >= (select filter1 from fd)
    and payment_timestamp <= (select filter3 from fd)
    and payment_status in ('paid','discarded') /*add status discarded for TTD order that deleted by cust in the same day|18 feb 21*/
)
, ocd as (
  select
    distinct
    order_id
    , order_detail_id
    , order_type
    , safe_cast(selling_price as float64) as selling_price
    , selling_currency
    , customer_price as customer_price_ocd
    , case
        when order_type = "flight"
          then order_name
        when order_type = "car"
          then order_name
        when order_type = "train"
          then 'PT. Kereta Api Indonesia'
        when order_type = "railink"
          then order_name
        when order_type in ("insurance","cancel_insurance")
          then order_name_detail
        else order_name
      end as order_detail_name
    , order_name
    , order_name_detail
    , order_master_id as product_provider
  from
    `datamart-finance.staging.v_order__cart_detail`
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp <= (select filter3 from fd)
    and order_type in ('flight','car','hotel','tixhotel','event','train','insurance','cancel_insurance','tix','airport_transfer','railink')
)
, ocd_or as (
  select
    order_id
    , order_type
    , order_detail_id
  from
    `datamart-finance.staging.v_order__cart_detail`
  where
    order_detail_status in ('active','refund','refunded','hide_by_cust')
    and created_timestamp >= (select filter2 from fd)
    and created_timestamp < (select filter3 from fd)
  group by
    1,2,3
)
, ocdrd as (
  select
    order_id
    , case
        when order_id is null then 0
        else 1
        end as is_reschedule
  from
    ocd_or
  where
    order_type in ('refund_deposit')
  group by 1
)
, oci as (
  select
    order_detail_id
    , string_agg(distinct issue_code) as insurance_issue_code
  from
    `datamart-finance.staging.v_order__cart_insurance` 
  group by
    1
)
, occi as (
  select
    order_detail_id
    , string_agg(distinct issue_code) as cancel_insurance_issue_code
  from
    `datamart-finance.staging.v_order__cart_cancel_insurance` 
  group by
    1
)
, hbi as (
  select
    safe_cast(itinerary_id as string) as hotel_itinerarynumber
    , safe_cast(sum(premium_total) as float64) as nha_isurance_value
  from
    `datamart-finance.staging.v_homes_booking_insurances`
  where
    premium_total <> 0
    and not is_deleted
    -- and booking_status = 'issued'
  group by
  1
)
, op as (
  select
    order_id
    , payment_source
    , datetime(payment_timestamp, 'Asia/Jakarta') as payment_timestamp_op
    , datetime(payment_lastupdate, 'Asia/Jakarta') as payment_lastupdate_op
  from
    `datamart-finance.staging.v_order__payment`
  where
    payment_timestamp >= (select filter2 from fd)
    and payment_timestamp <= (select filter3 from fd)
    and payment_flag = 1
    and payment_id = 1
  group by
    1,2,3,4
)
, occip as (
  select
    distinct
    order_detail_id
    , count(order_passenger_id) as total_pax_ci
  from
    (
      select
      distinct
        order_detail_id
        , order_passenger_id
      from
        `datamart-finance.staging.v_order__cart_cancel_insurance_pax` 
    )
  group by 
    order_detail_id
)
, wsr_id as (
  select
    distinct
    safe_cast(supplier_id as int64) as airlines_master_id
    , supplier_name
    , workday_supplier_reference_id
    , vendor
  from
    `datamart-finance.staging.v_workday_mapping_supplier`
  where
    vendor = 'na'
)
, wsr_name as (
  select
    distinct
    supplier_id as airlines_master_id
    , supplier_name
    , workday_supplier_reference_id
    , vendor
    , is_deposit_flag
  from
    `datamart-finance.staging.v_workday_mapping_supplier`
  where
    vendor = 'sa'
)
, ocfp as (
  select
    order_detail_id
    , string_agg(ticket_number order by order_passenger_id desc) as ticket_number
  from
    `datamart-finance.staging.v_order__cart_flight_passenger`
  group by
    order_detail_id
)
, ocfc as (
  select
    order_detail_id
    , round(total,0) as order_flight_commission
  from
    (
      select
        order_detail_id
        , safe_cast(total as float64) as total
        , row_number() over(partition by order_detail_id order by processed_dttm desc, updated_timestamp desc) as rn
      from
        `datamart-finance.staging.v_order__cart_flight_comission` 
      where
        created_timestamp >= (select filter2 from fd)
    )
  where rn = 1
) 
, fmd as (
  select 
    *
  from (
    select 
      order_detail_id
      , original_commission_amount	
      , manual_markup_amount
      , safe_cast(
          case 
            when manual_markup_amount > 0 then commission
            else original_commission_amount
          end 
        as float64) as commission_flight_fmd
      , row_number() over(partition by order_id, order_detail_id order by processed_dttm  desc) as rn
    from `datamart-finance.staging.v_flight_management_dashboard` 
    where date(payment_date) >= (select date(filter2) from fd)
  )  
  where rn = 1
)
, tfrro as (
  select
    distinct
    ro.orderId as old_order_id
    , ro.orderDetailId as old_order_detail_id
    , ro.newOrder.orderId as new_order_id
    , rof.orderDetailId as new_order_detail_id
    , rof.fareDetail.fareDiff as fare_diff
    , rof.fareDetail.taxDiff as tax_diff
    , rof.fareDetail.additionalIncomeDiff reschedule_fee
    , ro_old.isFlexi is_flexi
  from 
    `datamart-finance.staging.v_tix_flight_reschedule__reschedule_order` ro
    left join unnest(newOrder.orderDetail) as rof
    left join unnest(oldOrderDetails) ro_old on ro.orderDetailId = ro_old.orderDetailId
  where ro.rescheduleStatus = 'CLOSED'
  group by 1,2,3,4,5,6,7,8
)
, ocba as (
  select
     parent_id as order_detail_id
     , order_detail_id as halodoc_order_detail_id
     , sum(total_price) as halodoc_sell_price_amount
     , 1 as is_has_halodoc_flag
     , concat /*EDP 16 Nov 2021: new datasource for addons flight (rapid test)*/
      (
        '['
        , string_agg
          (
          concat
            (
              '{'
              ,'"vendor":"',vendor_addons,'",'
              ,'"desc":"',desc_addons,'",'
              ,'"value":',ifnull(total_price,0)
              ,'}'
            )
          )
        ,']'
      ) as addons_flight_json
  from
    (
    select
      order_detail_id
      , parent_id
      , safe_cast(json_extract_scalar(pj,'$.total_fee.') as numeric) as total_price
      , updated_at 
      , processed_dttm
      , json_extract_scalar(pj,'$.description.') as desc_addons
      , json_extract_scalar(pj,'$.vendor.') as vendor_addons
      , row_number() over (partition by order_detail_id, pj order by updated_at desc, processed_dttm desc) rn
    from 
      `datamart-finance.staging.v_order__cart_bundling_addons`
    left join
      unnest(json_extract_array(param_json)) as pj
    where status = 'issued'  
    )
  where
    rn = 1
  group by
    1,2
)
, ocbap as (
  select
    order_detail_id as halodoc_order_detail_id
    , count(distinct order_passenger_id) as halodoc_pax_count
  from
    (
      select
        order_detail_id
        , order_passenger_id 
        , updated_at 
        , processed_dttm
        , row_number() over (partition by order_detail_id,order_passenger_id order by updated_at desc ,processed_dttm desc) rn
    from 
      `datamart-finance.staging.v_order__cart_bundling_addons_passenger`
    group by
      1,2,3,4
  )
  where rn = 1
  group by
    1
)
, fs as (
  select
    order_detail_id
    , product_name as ancillary_product_name
    , customer_price as ancillary_customer_price
  from
  (
    select
      order_detail_id
      , [
          struct('baggage' as product_name, baggage_fee as customer_price)
          , struct('meals' as product_name, meal_fee as customer_price)
          , struct('seat_selection' as product_name, ifnull(seat_selection_fare,0) as customer_price)
        ] as ancillary
    from
      `datamart-finance.staging.v_order__cart_flight`
    left join
    (
      select
        order_detail_id
        , sum(seat_fare_depart+seat_fare_return) as seat_selection_fare
      from
        `datamart-finance.staging.v_order__cart_flight_passenger` ocfp
      inner join
        (
        select
          order_passenger_id
          , ifnull(sum(cast(json_extract_scalar(adj, '$.fare') as numeric)),0) AS seat_fare_depart
          , ifnull(sum(cast(json_extract_scalar(arj, '$.fare') as numeric)),0) AS seat_fare_return
        from
          (
            select
              *
            from
              (
                select
                  order_passenger_id
                  , addons_depart_json
                  , addons_return_json
                  , row_number() over(partition by order_passenger_addons_id order by updated_at desc) as rn
                from
                  `datamart-finance.staging.v_order__cart_flight_passenger_addons`
                where
                  updated_at >= (select filter2 from fd)
              )
            where rn = 1
          )
          left join unnest(json_extract_array(addons_depart_json)) as adj
          left join unnest(json_extract_array(addons_return_json)) as arj
        group by 1
        ) as seat
        using(order_passenger_id)
      group by 1
    ) using(order_detail_id)
    where departure_time >= (select filter2 from fd)
  )
  left join
    unnest(ancillary) a
  where a.customer_price <> 0
)
, fao as (
  select
    order_detail_id
    , concat
      (
        '['
        , string_agg
          (
          concat
            (
              '{'
              ,'"category":"',ancillary_product_name,'",'
              ,'"value":',ifnull(ancillary_customer_price,0)
              ,'}'
            )
          )
        ,']'
      ) as ancillary
    , safe_cast(sum(ancillary_customer_price) as numeric) as total_ancillary
  from
    fs
  group by 1
)
, ocf as (
  select
    distinct
    ocf.order_detail_id
    , ocf.booking_code
    , ocf.ticket_status
    , ocf.vendor
    , ocf.account
    , case
        when ocf.vendor = 'na' and ocf.account='tiketcomLionVedaleon' then 'VR-00017129'
        when ocf.vendor = 'sa' then wsr_name.workday_supplier_reference_id
        when ocf.vendor = 'na' then wsr_id.workday_supplier_reference_id
      end as airlines_master_id
    , safe_cast(ocf.balance_due as float64) as balance_due
    , safe_cast(ocf.price_nta as float64) as price_nta
    , safe_cast(ocf.count_adult+ocf.count_child+ocf.count_infant as int64) as total_pax
    , datetime(ocf.departure_time, 'Asia/Jakarta') as departure_time
    , safe_cast(ocf.baggage_fee as float64) as baggage_fee
    , ancillary as ancillary_flight_json
    , coalesce(safe_cast(total_ancillary as int64),0) as total_ancillary_flight
    , halodoc_sell_price_amount
    , halodoc_pax_count
    , is_has_halodoc_flag
    , addons_flight_json
    , case
        when ocf.vendor = 'sa' and wsr_name.is_deposit_flag is null then 'Non deposit'
        else 'Deposit'
      end as deposit_flag_flight
    , ticket_number
    , ocfc.order_flight_commission
    , manual_markup_amount
    , commission_flight_fmd
  from
    `datamart-finance.staging.v_order__cart_flight` ocf
    left join wsr_id using (airlines_master_id,vendor)
    left join wsr_name on ocf.account = wsr_name.supplier_name and ocf.vendor = 'sa'
    left join ocfp on ocf.order_detail_id = ocfp.order_detail_id
    left join ocfc on ocf.order_detail_id = ocfc.order_detail_id
    left join fmd on ocf.order_detail_id = fmd.order_detail_id
    left join ocba on ocf.order_detail_id = ocba.order_detail_id
    left join ocbap using (halodoc_order_detail_id)
    left join fao on ocf.order_detail_id = fao.order_detail_id
  where
    departure_time >= (select filter2 from fd)
)
/* EDP 01 Des,2021: adding new datasource for addons car data */
, addons_car as (
  select
    safe_cast(monolith_order_detail_id as int64) as order_detail_id
    , concat
      (
        '['
        , string_agg
          (
          concat
            (
              '{'
              ,'"category":"',lower(trim(additional_type)),'",'
              ,'"name":"',additional_name,'",'
              ,'"value":',ifnull(additional_price,0)
              ,'}'
            )
          )
        ,']'
      ) as addons_name
    , sum(additional_price) as total_addons
  from
    (
      select
        * except(rn)
      from
        (
          select
            order_id
            , monolith_order_detail_id
            , row_number() over(partition by monolith_order_id order by processed_dttm desc) rn
          from
            `datamart-finance.staging.v_car_rental_admin_vendor__order_car`
          where
            payment_timestamp >= (select filter2 from fd)
            and order_status in('ISSUED','REFUNDED')
        )
      where
        rn = 1
    ) as oc
  join
    (
      select
        * except(rn)
      from
        (
          select
            order_id
            , additional_price
            , additional_name
            , additional_type
            , row_number() over(partition by order_id, additional_type, additional_name, order_additional_id order by processed_dttm desc) rn
          from
            `datamart-finance.staging.v_car_rental_admin_vendor__order_breakdown_additional`
          where
            --date( processed_dttm ) >= (select date(datetime(filter2, 'Asia/Jakarta')) from fd)             
            --created_date >= (select filter2 from fd)
            processed_dttm >= (select filter2 from fd)
            and additional_price > 0
        )
      where
        rn = 1
    ) as oba
  using(order_id)
  group by 1
)
, occar as (
  select
    occar.order_detail_id
    , (timestamp_diff(max(occar.checkin_date),min(occar.checkin_date),day)+1) * max(occar.qty) as quantity_car
    , safe_cast(sum(occar.net_rate_price) as float64) as net_rate_price_car
    , string_agg(distinct occar.net_rate_currency) as net_rate_currency_car
    , string_agg(distinct split(split(log_data,'business_id":')[safe_offset(1)],',"')[safe_offset(0)])  as supplier_id_car
    , max(safe_divide(customer_price,sell_rate_price)) as kurs_car
    , string_agg(distinct addons_name) as addons_car_json
    , max(coalesce(total_addons,0)) as total_addons_car
    , datetime(min(occar.checkin_date), 'Asia/Jakarta') as min_checkin_date_car
  from 
    `datamart-finance.staging.v_order__cart_car` occar
    left join addons_car using(order_detail_id)
  where
    lastupdate >= (select filter2 from fd)
  group by 
    order_detail_id
)
, oct as (
  select
    order_detail_id
    , string_agg(distinct book_code) as booking_code_train
    , safe_cast(sum(net_adult_price*count_adult+net_child_price*count_child+net_infant_price*count_infant) as float64) as net_rate_price_train
    , safe_cast(sum(count_adult+count_child+count_infant) as float64) as quantity_train
    , datetime(min(arrival_datetime), 'Asia/Jakarta') as arrival_datetime_train
  from
    `datamart-finance.staging.v_order__cart_train`
  where
    departure_datetime >= (select filter2 from fd)
  group by order_detail_id
)
/* add railink @wahyu - 2020-12-04*/
, ocr as ( 
  select
    order_detail_id
    , string_agg(distinct book_code) as booking_code_railink
    , replace(train_name,' ','_') as product_provider_id_railink
    , train_name as product_provider_name
    , safe_cast(sum(net_adult_price*coalesce(count_adult,0)+net_child_price*coalesce(count_child,0)+net_infant_price*coalesce(count_infant,0)) as float64) as net_rate_price_railink
    , safe_cast(sum(coalesce(count_adult,0)+coalesce(count_child,0)+coalesce(count_infant,0)) as float64) as quantity_railink
    , datetime(min(arrival_datetime), 'Asia/Jakarta') as arrival_datetime_railink
  from
    `datamart-finance.staging.v_order__cart_railink`
  where
    departure_datetime >= (select filter2 from fd)
  and ticket_status = 'issued'
  group by order_detail_id,train_name
)
, event_order as ( /* new datasource TTD transactions */
  select
    * except
      (
        product_subcategories
        , ps
      )
    , string_agg(distinct lower(trim(json_extract_scalar(ps,'$.code')))) as product_subcategory
  from
  (
    select
      * except
        (
          quantity
          , commission
          , payment_to_vendor
          , partner_campaign_amount
          , partner_loyalty_amount
          , tiket_campaign_amount
          , tiket_loyalty_amount
          , rn
        )
      , sum(quantity) as quantity
      , sum(payment_to_vendor) as payment_to_vendor
      , sum(commission) as commission
      , sum(coalesce(partner_campaign_amount,0)) as partner_campaign_amount
      , sum(coalesce(partner_loyalty_amount,0)) as partner_loyalty_amount
      , sum(coalesce(tiket_campaign_amount,0)) as tiket_campaign_amount
      , sum(coalesce(tiket_loyalty_amount,0)) as tiket_loyalty_amount
    from
      (
        select
          safe_cast(coreorderid as int64) as order_id
          , json_extract_scalar (product_translations, '$.product_translations[0].title') as product_value
          , json_extract_scalar (product_productPartners, '$.product_productPartners[0].businessId') as product_business
          , trim(product_supplierCode) as product_supplier_code
          , json_extract_scalar (product_productPartners, '$.product_productPartners[0].name') as product_supplier
          , date(productSchedule_date) as product_schedule_date
          , datetime(productPackage_earliestAvailabilityDate, 'Asia/Jakarta') as profile_event_start
          , lower(trim(product_primaryCategory)) as product_category
          , lower(trim(product_pricingType)) as product_pricing_type
          , case
                when supplierOrderId is not null then concat(supplierOrderId,'-',t.code)
                else t.code
              end as ticket_number
          , product__id as product_id
          , lower(trim(product_productPartners_disbursement_type)) as product_partner_disbursement_type
          , currencyRate as currency_rate
          , product_currency as net_rate_currency_event
          , pt.code as tier
          , pt.quantity as quantity
          , safe_cast(pt.basePriceInCents.numberLong*pt.quantity/100 as float64) as payment_to_vendor
          , safe_cast(pt.commissionInCents.numberLong*pt.quantity/100 as float64) as commission
          , safe_cast(regexp_replace(json_extract(s,'$.partnerCampaignAmountInCents'),'[^0-9 ]','') as float64)/100*pt.quantity as partner_campaign_amount
          , safe_cast(regexp_replace(json_extract(s,'$.partnerLoyaltyAmountInCents'),'[^0-9 ]','') as float64)/100*pt.quantity as partner_loyalty_amount
          , safe_cast(regexp_replace(json_extract(s,'$.tiketCampaignAmountInCents'),'[^0-9 ]','') as float64)/100*pt.quantity as tiket_campaign_amount
          , safe_cast(regexp_replace(json_extract(s,'$.tiketLoyaltyAmountInCents'),'[^0-9 ]','') as float64)/100*pt.quantity as tiket_loyalty_amount
          , product_subcategories
          , row_number() over(partition by coreorderId, pt.code order by lastModifiedDate desc) as rn
        from 
          `datamart-finance.staging.v_events_v2_order__order_l2`
          left join unnest (priceTierQuantities) as pt
          left join unnest(tickets) as t
          left join unnest(json_extract_array(subsidies,'$.subsidies')) as s 
            on json_extract(s, '$.priceTierCode')=concat("\"", pt.code,"\"")
        where
          createdDate >= (select filter2 from fd)
          and createdDate <=(select filter3 from fd)
          and lastModifiedDate >= (select filter2 from fd)
          and status LIKE '%ISSUED%'
          /*and (status LIKE '%ISSUED%' or status = 'PAID' or status = 'BLOCKED')*/
      )
    where
      rn = 1
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
  )
  left join
    unnest(json_extract_array(product_subcategories,'$.product_subcategories')) as ps
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
, fact_ttd as (
  select
    order_id /*as order_detail_id*/
    , tier as tier_event
    , quantity as quantity_event
    ,	case
        when count(*) over(partition by order_id) > 1
          then concat(ticket_number, ' - ["', tier, '"]')
        else ticket_number
      end as tiket_barcode_event
    , /*case
        when product_category = 'hotel' then product_schedule_date
        else profile_event_start
      end as event_datetime*/
      product_schedule_date as event_datetime
    , product_value as event_name
    , case
        when product_category <> 'hotel' and product_pricing_type='commission' and commission <> 0
          then payment_to_vendor + commission
        else payment_to_vendor 
      end as payment_to_vendor_event
    , -1
      * (
            partner_campaign_amount
          + partner_loyalty_amount
          + tiket_campaign_amount
          + tiket_loyalty_amount
        ) as subsidy_event
    , case
        when product_pricing_type='mark_up' then commission
        else 0
      end as upselling_event
    , case
        when product_category = 'hotel' and product_pricing_type = 'commission' then commission
        else 0
      end as commission_event
    , case
        when product_category in ('attraction','playground') then 'Attraction'
        when product_category in ('beauty_wellness','class_workshop','culinary','food_drink','game_hobby','tour','travel_essential') then 'Activity'
        when product_category in('event','todo_online') then 'Event'
        when product_category = 'transport' and product_supplier = 'Railink' then 'Train' 
        when product_category = 'transport'
          and product_supplier_code not in ('S2','S3','S6')
          and 
          (
             product_value like '%Car Rental%'
            or
            product_subcategory like '%lepas-kunci%'
            or
            product_subcategory like '%lepas kunci%'
            or
            product_subcategory like '%city-to-city%'
            or
            product_subcategory like '%city to city%'
            or
            product_subcategory like '%airport%'
          )
          then 'Car' 
        when
          product_category = 'transport'
          and product_subcategory like '%lepas-kunci%'
          and product_value like '%Rental%Sewa Motor%'
          then 'Car'
        when product_category = 'transport' then 'Activity'
        when product_category = 'hotel' then 'Hotel'
        else product_category
      end as event_type_name
    , case
        when length(product_id) = 0 then '(blank)'
        when product_id is null then '(null)'
        else product_id
      end as product_provider_event
    , case
        when 
          (
            length(product_business) = 0
            or
            product_business is null
          )
          then
            case
              when product_supplier_code = 'S2' then '23196226' /*BMG*/
              when product_supplier_code = 'S3' then '33505623' /*ESD*/
              when product_supplier_code = 'S6' then '33505604' /*Klook*/
              when length(product_business) = 0 then '(blank)'
              when product_business is null then '(null)'
            end
        when product_supplier = 'Railink' then 'VR-00000026'
        else product_business
      end as supplier_id_event
    , currency_rate as kurs_event
    , net_rate_currency_event
    , case
        when product_supplier_code in ('S2','S6','S3') then 'Deposit'
        when product_partner_disbursement_type = 'deposit' then 'Deposit'
        else 'Non deposit'
      end as deposit_flag_event
  from
    event_order
)
, h_prop as ( /*New product category 'Hotel NHA' for non Hotel product*/
  select
    distinct
    publicId as public_id
    , hcp.name_en as property_type
    , case
        when lower(hcp.name_en) = 'hotel' 
            or lower(hcp.name_en) = 'hotel-unknown'
            or lower(hcp.name_en) = 'resort'
            or lower(hcp.name_en) = 'conference establishment'
            or lower(hcp.name_en) = 'heritage hotel'
            or lower(hcp.name_en) = 'love hotel'
            or lower(hcp.name_en) = 'motel'
            or lower(hcp.name_en) = 'inn'
            or lower(hcp.name_en) = 'bed & breakfast'
            or lower(hcp.name_en) = 'guest House'
            or lower(hcp.name_en) = 'all-inclusive'
            or lower(hcp.name_en) = 'hostel'
            or lower(hcp.name_en) = 'condominium resort'
            or lower(hcp.name_en) = 'hostal'
            or lower(hcp.name_en) = 'pousada (portugal)'
            or lower(hcp.name_en) = 'pousada (brazil)'
            or lower(hcp.name_en) = 'capsule hotel'
            or lower(hcp.name_en) = 'other'
            or lower(hcp.name_en) = 'kost'
            or lower(hcp.name_en) = 'homestay'
          then 'hotel'
        else 'nha'
      end as property_category
  from 
    `staging.v_hotel_core_hotel_neat` hn
    left join `datamart-finance.staging.v_hotel_core_property_type_flat` hcp
      on hn.propertyTypeId = safe_cast(hcp._id as int64)
    where hn.isActive = 1  
    and hn.isDeleted = 0  
)
, master_category_add_ons as (
  select
    add_ons_name as category_code
    , spend_category_name
  from 
    `datamart-finance.staging.v_workday_mapping_category_hotel_add_on`
)
, oth as (
  select
    distinct
    order_id
    , case
        when safe_cast(hotel_itinerarynumber as string) = '' or hotel_itinerarynumber is null then itinerary_id
        else hotel_itinerarynumber
      end as hotel_itinerarynumber
    , hotel_id as hotel_id_oth
    , datetime(booking_checkindate, 'Asia/Jakarta') as booking_checkindate
    , datetime(booking_checkoutdate, 'Asia/Jakarta') as booking_checkoutdate
    , safe_cast(nett_price as float64) as nett_price
    , round(rebooking_price) as rebooking_price_hotel
    , room_source
    , coalesce(json_extract_scalar(additional_info,'$.name'), room_source_info) as room_source_info
    , booking_room * booking_night as room_night
    , coalesce(markup_percentage,0) as markup_percentage_hotel
    , vendor_incentive
    , auto_subsidy_value
    , order_issued
  from
    `datamart-finance.staging.v_order__tixhotel`
    left join
     (
      select  
          max(safe_cast(orderId as int64)) order_id
          , max(itineraryId) as itinerary_id
          , string_agg(distinct json_extract_scalar(supplier,'$.supplier.name')) as room_source_info
       from  `datamart-finance.staging.v_hotel_cart_book` 
        where createdDate >= (select filter2 from fd)
        and lower(status) not in ('canceled','pending')
        group by orderId
     ) thi using(order_id)
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp <= (select filter3 from fd)
    -- and hotel_itinerarynumber is not null /*temporary solution for hotel_itinerarynumber null on ott/*
)
, hb as (
  select
    distinct
    safe_cast(id as string) as hotel_itinerarynumber
    , currency_exchange_rate as kurs
    , hotel_id
  from
    `datamart-finance.staging.v_hotel_bookings`
)
, hbd as (
  select
    safe_cast(itinerary_id as string) as hotel_itinerarynumber
    , string_agg(distinct net_rate_currency) as net_rate_currency
    , sum(total_net_rate_price) as total_net_rate_price
  from 
    (select
      distinct
      itinerary_id
      , checkin_date
      , case when net_rate_currency = 'IDR' then round(total_net_rate_price) else total_net_rate_price end as total_net_rate_price
      , net_rate_currency
    from `datamart-finance.staging.v_hotel_booking_details`) a
  group by itinerary_id
)
, hbao as (
  select 
    safe_cast(hbao.itinerary_id as string) as hotel_itinerarynumber
    , category_code
    , sum(amount) as add_ons_hotel_quantity
    , sum(round(total_net_rate_price,2)) as add_ons_hotel_net_price_amount
  from 
    `datamart-finance.staging.v_hotel_booking_add_ons` hbao
  group by
    1,2
)
, hbao_array as (
  select 
    hotel_itinerarynumber
    , array_agg(
        struct(
          category_code
          , coalesce(spend_category_name,'Spend Category is not defined')  as add_ons_spend_category
          , add_ons_hotel_quantity
          , add_ons_hotel_net_price_amount)) as add_ons_hotel_detail_array 
  from 
    hbao
    left join master_category_add_ons mcao using (category_code)
  group by 1
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
, oth_fact as (
  select
    order_id
    , case
        when room_source_info = 'Tiket Network Pte Ltd' then 'SGP'
        else 'IDN'
      end as intercompany_info
    , hotel_itinerarynumber
    , room_source
    , case
        when room_source = 'TIKET' then safe_cast(hotel_id as string)
        when room_source like '%EXPEDIA%' then 'VR-00000023'
        when room_source like '%HOTELBEDS%' then 'VR-00000024'
        when room_source like '%AGODA%' then 
          case 
            when room_source_info like '%BOOKING%' then 'VR-00015310'
            else 'VR-00000025'
          end
      end as hotel_id
    , case 
        when room_source = 'TIKET' then safe_cast(hotel_id as string)
        when room_source like '%EXPEDIA%' then 'EXPEDIA'
        when room_source like '%HOTELBEDS%' then 'HOTELBEDS'
        when room_source like '%AGODA%' then 
          case 
            when room_source_info like '%BOOKING%' then 'BOOKING.COM'
            else 'AGODA'
          end
      else room_source
      end as product_provider_hotel
    , hotel_id_oth
    , property_category
    , case
        when room_source = 'TIKET' then net_rate_currency
        else 'IDR'
      end as net_rate_currency_hotel
    , booking_checkindate
    , booking_checkoutdate
    , case
        when room_source = 'TIKET' then total_net_rate_price
        when room_source like '%AGODA%' then case when rebooking_price_hotel > 0 then rebooking_price_hotel - round(vendor_incentive*0.6) else nett_price - round(vendor_incentive*0.6) end /*update commission agoda(for tiket 60%,vendor 40%)*/
        when room_source like '%HOTELBEDS%' then case when rebooking_price_hotel > 0 then rebooking_price_hotel - round(safe_divide(markup_percentage_hotel*rebooking_price_hotel,(100+markup_percentage_hotel))) else nett_price end
        else case when rebooking_price_hotel > 0 then rebooking_price_hotel else nett_price end
      end as total_net_rate_price
    , coalesce(kurs,1) as kurs_hotel
    , room_night
    , case
        when hpt.type = 'deposit' then 'Deposit'
        else 'Non deposit'
      end as deposit_flag_hotel
    , ifnull(vendor_incentive,0) as vendor_incentive
    , ifnull(auto_subsidy_value,0) as auto_subsidy_value
    , order_issued
    , add_ons_hotel_detail_array
  from
    oth
    left join hb using (hotel_itinerarynumber)
    left join hbd using (hotel_itinerarynumber)
    left join hpt using (hotel_id)
    left join hbao_array using (hotel_itinerarynumber)
    left join h_prop on oth.hotel_id_oth = h_prop.public_id
)
, bp as (
  select
    business_id as product_provider_bp
    , business_name as business_name
  from
    `datamart-finance.staging.v_business__profile` 
)
, ac as (
  select
    distinct
    master_id as product_provider_ac
    , string_agg(distinct airlines_name) as airlines_name
  from
    `datamart-finance.staging.v_airlines_code`
  group by
    master_id
)
, cv as (
  select
    master_id
    , string_agg(distinct vendor_name) as vendor_name_car
  from
    `datamart-finance.staging.v_car__vendor`
  group by
    master_id
)
/* get data cogs for order tix*/
, octd as (
  select
    order_detail_id
    , sum(product_qty * product_seller_idr) as net_rate_price_tix
  from 
    `datamart-finance.staging.v_order__cart_tix_detail`
  group by
    1
)
, apt as (
  select 
    od.orderDetailId as order_detail_id
    , tr.totalunit as quantity_airport_transfer
    , tr.totalPrice as cogs_airport_transfer
    , UPPER(fleets.businessName) as supplier_id_airport_transfer
    , UPPER(fleets.businessName) as product_provider_id_airport_transfer
    , UPPER(pricingZoneName) as zone_airport_transfer
    , datetime(timestamp_millis(schedules.pickupDateTime.date),'Asia/Jakarta') as airport_transfer_pickup_datetime
  from 
    (
      select 
        orderdata.orderdetail
        , bookingdata.totalunit
        , bookingdata.price
        , bookingdata.totalPrice 
        , checkoutdata.fleets
        , checkoutdata.schedules
        , row_number() over(partition by _id order by updateddate desc) as rn 
      from 
        `datamart-finance.staging.v_apt_order__apt_order`
    ) tr
  , unnest (orderdetail) od with offset off_od
  , unnest (fleets) fleets with offset off_fleets
  , unnest (schedules) schedules with offset off_schedules
  , unnest(fleets.pricingZones) zone with offset off_zone
  where 
    rn = 1 
    and off_od = off_fleets and off_od = off_schedules
)
, apt_fact as (
  select
    safe_cast(order_detail_id as int64) as order_detail_id
    , quantity_airport_transfer
    , cogs_airport_transfer
    , supplier_id_airport_transfer
    , product_provider_id_airport_transfer
    , zone_airport_transfer
    , airport_transfer_pickup_datetime
  from
    apt
)
, combine as (
  select 
    * 
    , case 
        when ocd.order_type = 'flight' and is_reschedule = 1 and is_flexi and fare_diff+tax_diff+reschedule_fee > 0 then fare_diff+tax_diff+reschedule_fee --bypass dulu buat flexi nya
        else 0
      end as flexi_flight_price
    , case 
        when ocd.order_type in ('train') then concat(safe_cast(oc.order_id as string), ' - ', ocd.order_name_detail, ' / ', ocd.order_name)
        when ocd.order_type in ('railink') then concat(safe_cast(oc.order_id as string), ' - ', ocd.order_name_detail, ' / ', ocd.order_name)
        when ocd.order_type in ('flight') then concat(safe_cast(oc.order_id as string), ' / ', ocf.booking_code, ' - ', ocd.order_name_detail, ' / ', ocd.order_name, ' - ticket number : ', ifnull(ocf.ticket_number,'') )
        when ocd.order_type in ('tixhotel') then concat(safe_cast(oc.order_id as string), ' - ', ocd.order_name, ' / ', ocd.order_name_detail, ' - ', oth_fact.hotel_itinerarynumber)
        when ocd.order_type in ('event') then concat(safe_cast(oc.order_id as string), ' - ', ocd.order_name, ' / ', ocd.order_name_detail)
        when ocd.order_type in ('car') then concat(safe_cast(oc.order_id as string), ' - ', ocd.order_name_detail)
        when ocd.order_type in ('insurance') then concat(safe_cast(oc.order_id as string), ' - ', 'Cermati BCA Insurance, Issue Code: ', ifnull(oci.insurance_issue_code,''))
        when ocd.order_type in ('cancel_insurance') then concat(safe_cast(oc.order_id as string), ' - ', 'Cermati Anti Galau, Issue Code: ', ifnull(occi.cancel_insurance_issue_code,''))
        when ocd.order_type in ('tix') then concat(safe_cast(oc.order_id as string), ' - ', ocd.order_name_detail)
        when ocd.order_type in ('airport_transfer') then concat(safe_cast(oc.order_id as string), ' - ', order_name_detail, ' - ', zone_airport_transfer)
      end as memo
  from 
    oc
    inner join ocd using (order_id)
    left join op using (order_id)
    left join ocf using (order_detail_id)
    left join oth_fact using (order_id)
    left join occip using (order_detail_id)
    left join occar using (order_detail_id)
    left join oct using (order_detail_id)
    left join ocr using (order_detail_id)
    left join fact_ttd using(order_id)
    left join oci using (order_detail_id)
    left join occi using (order_detail_id)
    left join hbi using (hotel_itinerarynumber)
    left join apt_fact using (order_detail_id)
    left join bp on order_type not in ('flight','tixhotel') and ocd.product_provider = bp.product_provider_bp
    left join ac on order_type in ('flight') and ocd.product_provider = ac.product_provider_ac
    left join octd using (order_detail_id)
    left join ocdrd using(order_id)
    left join tfrro on ocd.order_detail_id = tfrro.new_order_detail_id
  where
    (order_type = 'flight' and ocf.ticket_status = 'issued')
    or
    (order_type = 'tixhotel' and oth_fact.order_issued = 1)
    or
    (order_type in ('insurance','cancel_insurance','car','train','event','tix','airport_transfer','railink'))
)
, fact_product as (
  select
    case
        when 
          /*string_agg(distinct reseller_type) in ('reseller','widget') and*/
          string_agg(distinct intercompany_info) = 'SGP'
          and max(payment_timestamp_oc) >= '2021-03-01' 
          /*and string_agg(distinct safe_cast(reseller_id as string)) = '34382690' /* for agoda as b2b, company on SI should be GTN_SGP. 09 March 2021 EDP */
          then 'GTN_SGP'
        else 'GTN_IDN'
    end as Company
    , case
        when string_agg(distinct order_type) in ('flight','train','tix','airport_transfer','railink') then 'IDR'
        when string_agg(distinct order_type) in ('tixhotel') then string_agg(distinct net_rate_currency_hotel)
        when string_agg(distinct order_type) in ('car') then string_agg(distinct net_rate_currency_car)
        when string_agg(distinct order_type) in ('event') then string_agg(distinct net_rate_currency_event)
        else null
      end as invoice_currency
    , case
        when string_agg(distinct order_type) = 'flight' then string_agg(distinct airlines_master_id)
        when string_agg(distinct order_type) = 'tixhotel' then string_agg(distinct hotel_id)
        when string_agg(distinct order_type) in ('car') then string_agg(distinct supplier_id_car)
        when string_agg(distinct order_type) in ('train') then 'VR-00000001'
        when string_agg(distinct order_type) in ('railink') then 'VR-00000026'
        when string_agg(distinct order_type) in ('event') then string_agg(distinct coalesce(new_supplier_id,supplier_id_event))
        when string_agg(distinct order_type) in ('tix') then '21229233'
        when string_agg(distinct order_type) in ('airport_transfer') then string_agg(distinct new_supplier_id)
        else null
      end as supplier_reference_id
    , max(payment_timestamp_oc) as invoice_date
    , order_id
    , order_detail_id
    , case
        when string_agg(distinct order_type) in ('flight', 'tix') then max(coalesce(payment_lastupdate_op,payment_timestamp_oc,payment_timestamp_op))
        when string_agg(distinct order_type) = 'tixhotel' then
          case when date(max(payment_timestamp_oc)) >= '2020-10-01' then max(booking_checkoutdate) -- @01 oct 2020, change due date tixhotel with checkoutdate (request accounting team)
          else max(booking_checkindate)
          end
        when string_agg(distinct order_type) = 'car' then max(min_checkin_date_car)
        when string_agg(distinct order_type) = 'train' then 
          case when date(max(payment_timestamp_oc)) >= '2020-04-01' then max(payment_timestamp_oc)
          else max(arrival_datetime_train)
          end
        when string_agg(distinct order_type) = 'railink' then max(arrival_datetime_railink)
        when string_agg(distinct order_type) = 'event' then max(event_datetime)
        when string_agg(distinct order_type) = 'airport_transfer' then max(airport_transfer_pickup_datetime)
        else null
      end as schedule_date
    , string_agg(distinct order_detail_name) as order_detail_name
    , case
        when string_agg(distinct order_type) = 'flight' then 'Ticket'
        when string_agg(distinct order_type) = 'tixhotel' then 'Room'
        when string_agg(distinct order_type) = 'car' then 'Rental'
        when string_agg(distinct order_type) = 'train' then 'Ticket'
        when string_agg(distinct order_type) = 'railink' then 'Ticket'
        when
          string_agg(distinct order_type) = 'event'
          and string_agg(distinct event_type_name) = 'Car'
          and string_agg(distinct combine.event_name) like '%Rental%Sewa Motor%'
          then 'Rental_Motor'
        when string_agg(distinct order_type) = 'event' then
          case
            when string_agg(distinct event_type_name) = 'Hotel' then 'Hotel_Voucher'
            when string_agg(distinct event_type_name) = 'Car' and 
              ( lower(string_agg(distinct event_type_name)) like ('%lepas kunci%') 
              or
              lower(string_agg(distinct event_type_name)) like ('%car%') )
              then 'Rental'
            when string_agg(distinct event_type_name) = 'Car' then 'Shuttle'
            when string_agg(distinct event_type_name) = 'Others' then 'Lounge'  
            else 'Ticket'
          end
        when string_agg(distinct order_type) = 'tix' then 'Tix_Redeem'
        when string_agg(distinct order_type) = 'airport_transfer' then 'Shuttle'
        else null
      end as spend_category
    , case
        when string_agg(distinct order_type) = 'flight' then max(total_pax)
        when string_agg(distinct order_type) = 'tixhotel' then max(room_night)
        when string_agg(distinct order_type) = 'car' then max(quantity_car)
        when string_agg(distinct order_type) = 'train' then max(quantity_train)
        when string_agg(distinct order_type) = 'railink' then max(quantity_railink)
        when string_agg(distinct order_type) = 'event' then max(quantity_event)
        when string_agg(distinct order_type) = 'tix' then 1
        when string_agg(distinct order_type) = 'airport_transfer' then max(quantity_airport_transfer)
        else null
      end as quantity
    , round(case
        when string_agg(distinct order_type) = 'flight' and flexi_flight_price > 0 then flexi_flight_price
        when string_agg(distinct order_type) = 'flight' 
          then sum(round(
            case
              when airlines_master_id = 'VR-00000011' and date(payment_timestamp_oc) >= '2021-10-15' and is_reschedule is null then price_nta - order_flight_commission /*trigana*/
              when airlines_master_id in ('VR-00000006','VR-00000011','VR-00000004','VR-00017129','VR-00000002') and date(payment_timestamp_oc) >= '2020-05-11' then price_nta /* 13 May 2020, Anggi Anggara: for lion group, start order >= 2020-05-11 using price_nta*//* 27 May 2020, Anggi Anggara: for trigana, sriwjaya , transnusa, start order >= 2020-05-11 using price_nta*//* 04 oct 2021, Garuda using price_nta, due to has commission*/
              when airlines_master_id in('VR-00000003') and date(payment_timestamp_oc) >= '2021-02-20' and manual_markup_amount > 0 then price_nta /* 20 feb 2021, for sabre with markup amount > 0 */
              when airlines_master_id in('VR-00000003','VR-00000007','VR-00000012') and date(payment_timestamp_oc) >= '2020-10-01' and is_reschedule is null then price_nta - order_flight_commission
              else balance_due
            end - total_ancillary_flight))
        when string_agg(distinct order_type) = 'tixhotel' then sum(total_net_rate_price-ifnull(nha_isurance_value,0))
        when string_agg(distinct order_type) = 'car' then sum(selling_price)-sum(ifnull(total_addons_car,0))
        when string_agg(distinct order_type) = 'train' then sum(net_rate_price_train)
        when string_agg(distinct order_type) = 'railink' then sum(net_rate_price_railink-(net_rate_price_railink*0.1))
        when string_agg(distinct order_type) = 'event' then sum(payment_to_vendor_event)
        when string_agg(distinct order_type) = 'tix' then max(net_rate_price_tix)
        when string_agg(distinct order_type) = 'airport_transfer' then max(cogs_airport_transfer)
        else null
      end,2) as total_line_amount
    , round(case
        when string_agg(distinct order_type) = 'flight' then 1
        when string_agg(distinct order_type) = 'tixhotel' then max(kurs_hotel)
        when string_agg(distinct order_type) = 'car' then max(kurs_car)
        when string_agg(distinct order_type) = 'train' then 1
        when string_agg(distinct order_type) = 'railink' then 1
        when string_agg(distinct order_type) = 'event' then max(kurs_event)
        when string_agg(distinct order_type) = 'tix' then 1
        when string_agg(distinct order_type) = 'airport_transfer' then 1
        else null
      end,2) as currency_conversion
    , case
        when string_agg(distinct order_type) = 'flight' then string_agg(booking_code order by order_detail_id)
        when string_agg(distinct order_type) = 'tixhotel' then string_agg(hotel_itinerarynumber order by order_detail_id)
        when string_agg(distinct order_type) = 'train' then string_agg(booking_code_train order by order_detail_id)
        when string_agg(distinct order_type) = 'railink' then string_agg(booking_code_railink order by order_detail_id)
        when string_agg(distinct order_type) = 'event' then string_agg(tiket_barcode_event order by order_detail_id)
        else null
      end as booking_code
    , case
        when string_agg(distinct order_type) = 'flight' then 'Flight'
        when date(max(payment_timestamp_oc)) >= '2020-11-21' and string_agg(distinct order_type) = 'tixhotel' and string_agg(property_category) = 'nha' then 'Hotel_NHA' /*21Nov2020: separate hotel & NHA hotel*/
        when string_agg(distinct order_type) = 'tixhotel' then 'Hotel'
        when string_agg(distinct order_type) = 'car' then 'Car'
        when string_agg(distinct order_type) = 'train' then 'Train'
        when string_agg(distinct order_type) = 'railink' then 'Train'
        when string_agg(distinct order_type) = 'event' then string_agg(distinct event_type_name)
        when string_agg(distinct order_type) = 'tix' then 'Tixpoint'
        when string_agg(distinct order_type) = 'airport_transfer' then 'Car'
        else null
      end as product_category 
    , case
        when string_agg(distinct order_type) = 'tixhotel' then string_agg(distinct product_provider_hotel)
        when string_agg(distinct order_type) = 'car' then string_agg(distinct supplier_id_car)
        when string_agg(distinct order_type) = 'event' then string_agg(distinct coalesce(new_product_provider_id,product_provider_event))
        when string_agg(distinct order_type) = 'train' then 'KAI'
        when string_agg(distinct order_type) = 'railink' then string_agg(distinct product_provider_id_railink)
        when string_agg(distinct order_type) = 'tix' then 'Tiketpoint_redemeed'
        when string_agg(distinct order_type) = 'airport_transfer' then string_agg(distinct new_product_provider_id)
        else string_agg(distinct safe_cast(product_provider as string))
      end as product_provider
    , case
        when string_agg(distinct order_type) = 'flight' then string_agg(distinct deposit_flag_flight)
        when string_agg(distinct order_type) = 'tixhotel' then string_agg(distinct deposit_flag_hotel)
        when string_agg(distinct order_type) = 'car' then 'Non deposit'
        /*  case --EDP, NOV 22,2021: accordingly commercial team (sugeng), this logic has not been used again since mid of 2020
            when string_agg(distinct order_detail_name) like '%EXTRA%' or string_agg(distinct order_detail_name) like '%BEST PRICE%' then 'Deposit'
            else 'Non deposit'
          end */
        when string_agg(distinct order_type) = 'train' then 'Deposit'
        when string_agg(distinct order_type) = 'railink' then 'Deposit'
        when string_agg(distinct order_type) = 'event' then string_agg(distinct coalesce(mend.deposit_flag,deposit_flag_event))
        when string_agg(distinct order_type) = 'tix' then 'Non deposit'
        /*when string_agg(distinct order_type) = 'airport_transfer' then 'Deposit'*/ /*EDP, NOV 1,2021 update payment type AT with non deposit - req Alghi*/
        when date(max(payment_timestamp_oc)) >= '2021-11-01' and string_agg(distinct order_type) = 'airport_transfer' then 'Non deposit'
        else null
      end as deposit_flag
    , case
        when string_agg(distinct order_type) = 'event' then string_agg(distinct order_detail_name)
        else null
      end as event_name
    , string_agg(distinct memo) as memo
    , sum(round(baggage_fee)) as baggage_fee
    , sum(total_ancillary_flight) as total_ancillary_flight
    , string_agg(distinct ancillary_flight_json) as ancillary_flight_json
    , sum(halodoc_sell_price_amount) as halodoc_sell_price_amount
    , string_agg(distinct json_extract_scalar(addons_flight_json, '$[0].desc')) as halodoc_desc
    , string_agg(distinct addons_flight_json) as addons_flight_json
    , sum(halodoc_pax_count) as halodoc_pax_count
    , array_concat_agg(add_ons_hotel_detail_array) as add_ons_hotel_detail_array
    , string_agg(distinct addons_car_json) as addons_car_json
    , sum(coalesce(total_addons_car,0)) as total_addons_car
    /* 25 May 2020: add customer reference id for SI, for B2C use value 'C-000001'*/
    , case
          when date(max(payment_timestamp_oc)) >= '2020-04-06' and string_agg(distinct corporate_flag) is not null and string_agg(distinct payment_source)  in ('cash_onsite','corporate_deposit','corporate_invoice','corporate_billing') then string_agg(distinct business_id) /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when date(max(payment_timestamp_oc)) <'2020-04-06' and string_agg(distinct corporate_flag) is not null then string_agg(distinct business_id) /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/ /*2021=07-18 - adding new payment source for b2b corporate*/
          when 
            date(max(payment_timestamp_oc)) >= '2020-04-06' and string_agg(distinct reseller_type) in ('none','online_marketing','native_apps')
            and (string_agg(distinct corporate_flag) is null or (string_agg(distinct corporate_flag) is not null and string_agg(distinct payment_source) not in ('cash_onsite','corporate_deposit','corporate_invoice','corporate_billing')))
              then 'C-000001'
          when 
            date(max(payment_timestamp_oc)) < '2020-04-06' and string_agg(distinct reseller_type) in ('none','online_marketing','native_apps')
            and (string_agg(distinct corporate_flag) is null)
              then 'C-000001'
          when string_agg(distinct reseller_type) in ('none','online_marketing','native_apps') then 'C-000001'
          when string_agg(distinct reseller_type) in ('tiket_agent','txtravel','agent','affiliate') then string_agg(distinct safe_cast(reseller_id as string))
          when string_agg(distinct reseller_type) in ('reseller','widget') then string_agg(distinct safe_cast(reseller_id as string))
        end as customer_reference_id
  from
    combine
    left join master_event_supplier mes on mes.old_supplier_id = coalesce(combine.supplier_id_event,combine.supplier_id_airport_transfer) and combine.order_name = mes.event_name and date(combine.payment_timestamp_oc) >= mes.start_date and date(combine.payment_timestamp_oc) <= mes.end_date
    left join master_event_product_provider mpp on mpp.old_product_provider_id = coalesce(combine.product_provider_event,combine.product_provider_id_airport_transfer) and combine.order_name = mes.event_name and date(combine.payment_timestamp_oc) >= mes.start_date and date(combine.payment_timestamp_oc) <= mes.end_date
    left join master_event_name_deposit mend on mend.event_name = combine.event_name and date(combine.payment_timestamp_oc) >= mend.start_date and date(combine.payment_timestamp_oc) <= mend.end_date
  where
    order_type in ('flight','tixhotel','car','train','event', 'tix','airport_transfer','railink')
  group by 
    order_id
    , order_detail_id
    , flexi_flight_price
    , tier_event
)
/* save the result of this query to temporary table -> let's agree the temporary location will be in `datamart-finance.datasource_workday.temp_supplier_invoice_raw_part_1`*/
select
  *
from fact_product