with
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -1 day) as filter2
    , timestamp_add(filter1, interval 3 day) as filter3
    , timestamp_add(filter1, interval -365 day) as filter4
  from
  (
    select
       timestamp_add(timestamp(date(current_timestamp(),'Asia/Jakarta')), interval -79 hour) as filter1
  )
)
, ocd_or as (
  select
    order_id
    , order_type
    , order_detail_id
    , replace(order_name,'"','') as order_name
    , order_name_detail
    , customer_price
    , selling_currency
    , selling_price
    , order_master_id
  from
    `datamart-finance.staging.v_order__cart_detail`
  where
    order_detail_status in ('active','refund','refunded','hide_by_cust')
    and created_timestamp >= (select filter2 from fd)
    and created_timestamp < (select filter3 from fd)
  group by
    1,2,3,4,5,6,7,8,9
)
, master_data_supplier as (
  select
    * except(processed_date)
  , case 
      when lead(date_add(processed_date, interval -1 day)) over(partition by Supplier_Reference_ID order by processed_date desc) is null
        then date('2000-01-01')
      else date_add(processed_date, interval -1 day)
    end as start_date
  , date_add(processed_date, interval -1 day) as active_date
  , coalesce(lag(date_add(processed_date, interval -2 day)) over(partition by Supplier_Reference_ID order by processed_date desc), date('2099-12-31')) as end_date
from
    `datamart-finance.datasource_workday.master_data_supplier`
)
, master_data_product_provider as (
  select
    * except(processed_date)
  , case 
      when lead(date_add(processed_date, interval -1 day)) over(partition by Organization_Reference_ID order by processed_date desc) is null
        then date('2000-01-01')
      else date_add(processed_date, interval -1 day)
    end as start_date
  , date_add(processed_date, interval -1 day) as active_date
  , coalesce(lag(date_add(processed_date, interval -2 day)) over(partition by Organization_Reference_ID order by processed_date desc), date('2099-12-31')) as end_date
from
    `datamart-finance.datasource_workday.master_data_product_provider`
)
, master_data_customer as (
  select
    * except(processed_date)
  , case 
      when lead(date_add(processed_date, interval -1 day)) over(partition by Customer_Reference_ID order by processed_date desc) is null
        then date('2000-01-01')
      else date_add(processed_date, interval -1 day)
    end as start_date
  , date_add(processed_date, interval -1 day) as active_date
  , coalesce(lag(date_add(processed_date, interval -2 day)) over(partition by Customer_Reference_ID order by processed_date desc), date('2099-12-31')) as end_date
from
    `datamart-finance.datasource_workday.master_data_customer`
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
  from
    `datamart-finance.staging.v_workday_mapping_supplier`
  where
    vendor = 'sa'
)
, tix_gcc as (
  select
    safe_cast(orderid as int64) as order_id
    , string_agg(distinct replace(voucherDescription,'\n',' ')) as giftcard_voucher
    , string_agg(distinct voucherPurpose) as giftcard_voucher_purpose
    , string_agg(distinct userEmailRefId) as giftcard_voucher_user_email_reference_id
  from 
    `datamart-finance.staging.v_tix_gift_voucher_gift_voucher` 
  where
    orderid is not null
  group by
    orderid 
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
, ops as (
  select
    order_id
    , is_main_payment
    , payment_source order_type
    , payment_name order_name
    , payment_name_detail order_name_detail
    , payment_amount customer_price
    , case 
        when lower(payment_source)='pay_later' and extra_fee > 0 then 0
        else extra_fee end as extra_fee
  from 
    `datamart-finance.staging.v_order__payment`
  where
    data_source = 'spanner'
    and payment_timestamp >= (select filter2 from fd)
    and payment_timestamp < (select filter3 from fd)
)
, oci as (
  select
    order_detail_id
    , parent_id
    , string_agg(distinct issue_code) as insurance_issue_code
  from
    `datamart-finance.staging.v_order__cart_insurance` 
  group by
    1,2
)
/* Fact Train */
, oct as (
  select
    order_detail_id
    , string_agg(distinct book_code) as booking_code_train
    , safe_cast(sum(net_adult_price*count_adult+net_child_price*count_child+net_infant_price*count_infant) as float64) as cogs_train
    , safe_cast(sum(count_adult+count_child+count_infant) as float64) as quantity_train
    , datetime(min(arrival_datetime), 'Asia/Jakarta') as arrival_datetime_train
    , string_agg(distinct ticket_status) as ticket_status_train
    , sum(extra_fee_price) as extra_fee_price
  from
    `datamart-finance.staging.v_order__cart_train`
  where
    departure_datetime >= (select filter2 from fd)
  group by order_detail_id
)
, fact_train as (
  select
    order_detail_id
    , booking_code_train
    , cogs_train
    , quantity_train
    , 'Train' as product_category_train
    , round(extra_fee_price * 10 / 11,0) as commission_train
    , round(extra_fee_price * 1 / 11,0) as vat_out_train
    , 0 as subsidy_train
    , 0 as upselling_train
    , 'VR-00000001' as supplier_train
    , 'KAI' as product_provider_train
    , 'Ticket' as revenue_category_train
    , ticket_status_train
  from
    oct
)
/* Fact Railink */
, ocr as (
  select
    order_detail_id
    , replace(train_name,' ','_') as product_provider_id
    , train_name as product_provider_name
    , string_agg(distinct book_code) as booking_code_railink
    , safe_cast(sum(net_adult_price*coalesce(count_adult,0)+net_child_price*coalesce(count_child,0)+net_infant_price*coalesce(count_infant,0)) as float64) as cogs_railink
    , safe_cast(sum(count_adult+coalesce(count_child,0)+coalesce(count_infant,0)) as float64) as quantity_railink
    , datetime(min(arrival_datetime), 'Asia/Jakarta') as arrival_datetime_railink
    , string_agg(distinct ticket_status) as ticket_status_railink
    , sum(extra_fee_price) as extra_fee_price
  from
    `datamart-finance.staging.v_order__cart_railink`
  where
    departure_datetime >= (select filter2 from fd)
  and ticket_status = 'issued'
  group by 1,2,3
)
, fact_railink as (
  select
    order_detail_id
    , booking_code_railink
    , cogs_railink-(cogs_railink*0.1) as cogs_railink
    , quantity_railink
    , 'Train' as product_category_railink
    , round((cogs_railink-(cogs_railink*0.1))*0.10101) as commission_railink
    , safe_cast((cogs_railink-(cogs_railink*0.1))*0.10101*0.1 as INT64) as vat_out_railink
    , 0 as subsidy_railink
    , 0 as upselling_railink
    , 'VR-00000026' as supplier_railink
    , product_provider_id as product_provider_railink
    , 'Ticket' as revenue_category_railink
    , ticket_status_railink
  from
    ocr
)
/* Fact Car */
, occar as (
  select
    order_detail_id
    , string_agg(distinct split(split(log_data,'business_id":')[safe_offset(1)],',"')[safe_offset(0)]) as vendor
    , sum(qty) as quantity_car
  from
    `datamart-finance.staging.v_order__cart_car` 
  where
    lastupdate >= (select filter2 from fd)
  group by
    order_detail_id
    , ticket_status
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
            processed_dttm >= (select filter2 from fd)
            and additional_price > 0
        )
      where
        rn = 1
    ) as oba
  using(order_id)
  group by 1
)
, fact_car as (
  select
    order_detail_id
    , vendor as product_provider_car
    , vendor as supplier_car
    , addons_name as addons_car_json
    , coalesce(total_addons,0) as total_addons_car
    , 0 as commission_car
    , 0 as upselling_car
    , 0 as subsidy_car
    , quantity_car
    , 'Rental' as revenue_category_car
    , 'Car' as product_category_car
  from
    occar
    left join addons_car using(order_detail_id)
)
, event_order as ( /* new datasource ttd transactions */
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
            , partner_campaign_amount
            , partner_loyalty_amount
            , tiket_campaign_amount
            , tiket_loyalty_amount
            , rn
          )
        , sum(quantity) as quantity
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
            , lower(trim(product_primaryCategory)) as product_category
            , lower(trim(product_pricingType)) as product_pricing_type
            , case
                when supplierOrderId is not null then concat(supplierOrderId,'-',t.code)
                else t.code
              end as ticket_number
            , product__id as product_id
            , pt.quantity as quantity
            , safe_cast(pt.commissionInCents.numberLong*pt.quantity/100*currencyRate as int64) as commission
            , safe_cast(safe_cast(regexp_replace(json_extract(s,'$.partnerCampaignAmountInCents'),'[^0-9 ]','') as float64)/100*currencyRate*pt.quantity as int64) as partner_campaign_amount
            , safe_cast(safe_cast(regexp_replace(json_extract(s,'$.partnerLoyaltyAmountInCents'),'[^0-9 ]','') as float64)/100*currencyRate*pt.quantity as int64) as partner_loyalty_amount
            , safe_cast(safe_cast(regexp_replace(json_extract(s,'$.tiketCampaignAmountInCents'),'[^0-9 ]','') as float64)/100*currencyRate*pt.quantity as int64) as tiket_campaign_amount
            , safe_cast(safe_cast(regexp_replace(json_extract(s,'$.tiketLoyaltyAmountInCents'),'[^0-9 ]','') as float64)/100*currencyRate*pt.quantity as int64) as tiket_loyalty_amount
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
        )
      where
        rn = 1
      group by
        1,2,3,4,5,6,7,8,9,10
    )
  left join
    unnest(json_extract_array(product_subcategories,'$.product_subcategories')) as ps
  group by
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
, fact_ttd as ( /* new fact event from microservices */
  select
    order_id /* as order_detail_id*/
    , quantity as quantity_event
    ,	ticket_number as ticket_number_event
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
    , product_supplier_code as ext_source_event
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
      end as product_category_event
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
              when product_supplier_code = 'S2' then '23196226'
              when product_supplier_code = 'S3' then '33505623'
              when product_supplier_code = 'S6' then '33505604'
              when length(product_business) = 0 then '(blank)'
              when product_business is null then '(null)'
            end
        when product_supplier = 'Railink' then 'VR-00000026'
        else product_business
      end as supplier_event
    , case
        when product_category = 'hotel' then 'Hotel_Voucher'
        when
          product_category = 'transport'
          and product_subcategory like '%lepas-kunci%'
          and product_value like '%Rental%Sewa Motor%'
          then 'Rental_Motor'
        when product_category = 'transport' and product_subcategory like '%lepas-kunci%' then 'Rental'
        when product_category = 'transport' and product_subcategory like '%lepas kunci%' then 'Rental'
        when product_category = 'transport' and product_value like '%Car Rental%' then 'Rental'
        when product_category = 'transport' and product_supplier_code not in ('S2','S3','S6') and 
          (
            product_subcategory like '%airport%'
            or
            product_subcategory like '%city-to-city%'
            or
            product_subcategory like '%city to city%'
          )
          then 'Shuttle'
        else 'Ticket'
      end as revenue_category_event
  from
    event_order
)
/* Fact Flight */
, ocdis as (
  select 
    order_detail_id
    , sum(coalesce(customer_price, 0)) as discount_amount
    , max(discount_type) discount_type
  from `datamart-finance.staging.v_order__discount` 
  where customer_price*-1 > 1000 /* 28 April 2021, discount < 1000 has reduced balance_due, sample id: 114283044*/
    group by 1
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
    order_detail_id
    , string_agg(distinct 
      case
        when ocf.vendor = 'na' and ocf.account='tiketcomLionVedaleon' then 'VR-00017129'
        when ocf.vendor = 'sa' then wsr_name.workday_supplier_reference_id
        when ocf.vendor = 'na' then wsr_id.workday_supplier_reference_id
      end) as supplier_flight
    , string_agg(distinct ticket_status) as ticket_status_flight
    , sum(round(
        case 
          when ocf.airlines_master_id not in (20865) then 
            case
              when price_total = 0 
                or price_total is null 
                  then price_adult + price_child + price_infant + ifnull(total_ancillary,0)
              else price_total
            end 
            + ifnull(sub_price_IDR, 0)
            - ifnull(balance_due, 0)
          else 0
        end
      )) as commission_flight /* Commmission except Citilink*/ /* Add Garuda since 08 Feb 2021*/ 
    , sum(round(
        case 
          when ocf.airlines_master_id not in (20865) then 
            case
              when price_total = 0 
                or price_total is null 
                  then price_adult + price_child + price_infant + ifnull(total_ancillary,0)
              else price_total
            end 
            + ifnull(sub_price_IDR, 0)
            - ifnull(price_nta, 0)
          else 0
        end
      )) as commission_price_nta_flight /* 13 May 2020, Anggi Anggara: for lion group, start order >= 2020-05-11 using price_nta*//* 27 May 2020, Anggi Anggara: for trigana, sriwjaya , transnusa, start order >= 2020-05-11 using price_nta*/
      /*04 Oct 2021, take out garuda (since 30 sept 2021 garuda has a commission from price_nta)*/
    , sum(round(
        case 
          when ocf.airlines_master_id in (20865) then 
            case
              when price_total = 0 
                or price_total is null 
                  then price_adult + price_child + price_infant + ifnull(total_ancillary,0)
              else price_total
            end 
            + ifnull(sub_price_IDR, 0)
            - ifnull(balance_due, 0)
          else 0
        end
      )) as upselling_flight /* Upselling only for flight Citilink*/ /* Add Garuda since 08 Feb 2021*/
      /*04 Oct 2021, take out garuda (since 30 sept 2021 garuda has a commission not markup)*/
    , sum(round(ifnull(balance_due,0) - ifnull(total_ancillary,0))) as cogs_flight
    , sum(round(ifnull(price_nta,0) - ifnull(total_ancillary,0))) as cogs_price_nta_flight /* 13 May 2020, Anggi Anggara: for lion group, start order >= 2020-05-11 using price_nta*//* 27 May 2020, Anggi Anggara: for trigana, sriwjaya , transnusa, start order >= 2020-05-11 using price_nta*/
    , sum(round(baggage_fee)) as baggage_fee
    , string_agg(distinct ancillary) as ancillary_flight_json
    , max(ifnull(total_ancillary,0)) as total_ancillary_flight
    , max(count_adult) + max(count_child) + max(count_infant) as quantity_flight
    , string_agg(distinct booking_code) as booking_code_flight
    , sum(sub_price_idr) as subsidy_flight
  from
    `datamart-finance.staging.v_order__cart_flight` ocf
    left join wsr_id using (airlines_master_id,vendor)
    left join wsr_name on ocf.account = wsr_name.supplier_name and ocf.vendor = 'sa'
    left join ocdis using (order_detail_id) /* 25 maret 2021, join ocdis to get discount member */
    left join fao using (order_detail_id)
  where
    departure_time >= (select filter2 from fd)
  group by
    order_detail_id
)
, ocfp as (
  select
    order_detail_id
    , string_agg(safe_cast(order_passenger_id as string) order by order_passenger_id) as order_passenger_id
    , string_agg(ticket_number order by order_passenger_id desc) as ticket_number_flight
  from
    `datamart-finance.staging.v_order__cart_flight_passenger`
  group by
    order_detail_id
)
, ores as (
  select
    new_order_id as order_id
    , old_order_detail_id as flight_reschedule_old_order_detail_id
    , string_agg(safe_cast(old_order_passenger_id as string) order by old_order_passenger_id asc) as reschedule_passenger_id
  from
    (
      select
        distinct
          new_order_id
          , old_order_id
          , old_order_detail_id
          , old_order_passenger_id
      from
        `datamart-finance.staging.v_order__reschedule` 
      where
        payment_timestamp >= (select filter2 from fd)
      and payment_status = 'paid'
     )
  group by
    1,2
)
, tfrro as (
  select
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
, rrbc as (
  select
   order_id
   , reschedule_passenger_id
   , reschedule_fee_flight
   , refund_amount_flight
   , reschedule_cashback_amount
   , reschedule_promocode_amount
  from
    (
      select
        refund_id
        , order_id
        , string_agg(passengerid order by passengerid asc) as reschedule_passenger_id
        , max(reschedulefee) as reschedule_fee_flight
        , max(totalrefundprice) as refund_amount_flight
        , max(reschedule_cashback_amount) as reschedule_cashback_amount
        , max(totalPromocode) as reschedule_promocode_amount
      from
      (
        select
          _id as refund_id
          , rescheduleOrderId as order_id
          , passengerId
          , rescheduleFee
          , refundstatus
          , rescheduleCashback + totalCashBackTix as reschedule_cashback_amount
          , totalrefundprice
          , totalPromocode
        from
          `datamart-finance.staging.v_tixerefund_refund_request_by_customer`
        where
          isreschedule = true
        group by
          1,2,3,4,5,6,7,8
      )
      group by 1,2
    )
  group by 1,2,3,4,5,6
)
, ocd_reschedule as (
  select
    order_detail_id
    , safe_cast(order_master_id as string) as product_provider_reschedule_flight
  from
    `datamart-finance.staging.v_order__cart_detail` 
  where
    created_timestamp >= (select filter4 from fd)
    and order_type = 'flight'
  group by 
    1,2
)
, ocf_reschedule as (
  select
    order_detail_id as flight_reschedule_old_order_detail_id
    , string_agg(distinct 
      case
        when ocf.vendor = 'na' and ocf.account='tiketcomLionVedaleon' then 'VR-00017129'
        when ocf.vendor = 'sa' then wsr_name.workday_supplier_reference_id
        when ocf.vendor = 'na' then wsr_id.workday_supplier_reference_id
      end) as supplier_reschedule_flight
    , string_agg(distinct product_provider_reschedule_flight) as product_provider_reschedule_flight
  from
    `datamart-finance.staging.v_order__cart_flight` ocf
    left join wsr_id using (airlines_master_id,vendor)
    left join wsr_name on ocf.account = wsr_name.supplier_name and ocf.vendor = 'sa'
    left join ocd_reschedule using (order_detail_id)
  where 
    departure_time >= (select filter4 from fd)
  group by 
    1
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
, ocdba as (
  select
    order_detail_id as halodoc_order_detail_id
    , order_name as halodoc_detail_name
  from
    ocd_or
  where
    order_type = 'bundling_addons'
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
  from(
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
, fact_flight as (
  select
    order_detail_id
    , quantity_flight
    , subsidy_flight * -1 as subsidy_flight
    , discount_amount as subsidy_discount
    , case
        when discount_type = 1 then 'Loyalty_Subsidy'
        when discount_type = 2 then 'Marketing_Subsidy'
        when discount_type = 3 then 'Dynamic_Subsidy'
        else null
      end as subsidy_category /* 01 April 2021, breakdown rev category subsidy by ocdis.discount_type */
    , cogs_flight
    , cogs_price_nta_flight
    , commission_flight
    , commission_price_nta_flight
    , order_flight_commission
    , baggage_fee
    , ancillary_flight_json
    , total_ancillary_flight
    , booking_code_flight
    , ticket_number_flight
    , ticket_status_flight
    , supplier_flight
    , upselling_flight
    , halodoc_sell_price_amount
    , halodoc_pax_count
    , is_has_halodoc_flag
    , addons_flight_json
    , 'Ticket' as revenue_category_flight
    , 'Flight' as product_category_flight
  from
    ocf
    left join ocfp using (order_detail_id)
    left join ocba using (order_detail_id)
    left join ocbap using (halodoc_order_detail_id)
    left join ocdba using (halodoc_order_detail_id)
    left join ocfc using (order_detail_id)
    left join ocdis using (order_detail_id)
    
)
/* Fact Hotel */
/*EDP 01 Dec 2021, adding new datasource from table v_hotel_cart_book to handling null data (entity & rebooking data)*/
, hcb as (
  select
    safe_cast(OrderId as int64) as order_id
    , itineraryId as itinerary_id
    , safe_cast(prevOrderId as int64) as old_id_rebooking
    , safe_cast(-1*priceDifference as float64) diff_amount_rebooking
    , json_extract_scalar(supplier,'$.supplier.name') as room_source_info
    , lower(confirmStatus) as confirm_status
    , lower(status) as status
  from
    `datamart-finance.staging.v_hotel_cart_book`
  where
    createdDate >= (select filter2 from fd)
    and lower(status) not in ('canceled','pending') /* takeout cancelled and pending transaction it makes the order id =0 */
)
, mst_prop as ( /*New product category 'Hotel NHA' for non Hotel product /applies to data >=21 Nov 2020 */
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
    , revenue_category_name
    , commission_category_name
  from 
    `datamart-finance.staging.v_workday_mapping_category_hotel_add_on`
)
, ott as (
  select
    distinct
    order_id
    , string_agg(distinct hotel_id) as hotel_id_oth
    , case when max(safe_cast(hotel_itineraryNumber as string))='' or max(hotel_itineraryNumber) is null then max(thi.itinerary_id) 
      else max(hotel_itineraryNumber) end as itinerary_id
    , string_agg(distinct room_source) as room_source
    , string_agg(distinct coalesce(json_extract_scalar(additional_info,'$.name'), room_source_info)) as room_source_info
    , max(created_timestamp) as created_timestamp
    , datetime(max(booking_checkoutdate), 'Asia/Jakarta') as booking_checkoutdate
    , max(nett_price) as nett_price
    , max(booking_room) * max(booking_night) as quantity
    , max(vendor_incentive) as vendor_incentive_hotel
    , max(case
        when room_source = 'HOTELBEDS' then case when round(rebooking_price,0) > 0 then round(safe_divide(coalesce(markup_percentage,0)*rebooking_price,(100+coalesce(markup_percentage,0)))) else totalwithtax - nett_price end
        else 0
      end) as upselling_hotel
    , max(subsidy_price) as subsidy_price
    , max(coalesce(markup_percentage,0)) as markup_percentage_hotel
    , max(auto_subsidy_value) as auto_subsidy_value
    , string_agg(distinct payment_type) as hotel_payment_type
    , round(max(rebooking_price),0) as rebooking_price_hotel
    , max(order_issued) as is_hotel_issued_flag
  from
    `datamart-finance.staging.v_order__tixhotel` 
 /*EDP 01 Dec 2021, handling null addittional_info data on table order__tixhotel*/
 left join 
      (
      select  
        order_id
        , max(itinerary_id) as itinerary_id
        , string_agg(distinct room_source_info) as room_source_info
      from
        hcb
      group by 1
    ) thi using(order_id) 
  where
    created_timestamp >= (select filter2 from fd)
  group by
    order_id
)
, hb as (
  select
    distinct
    safe_cast(id as string) as itinerary_id
    , safe_cast(hotel_id as string) as hotel_id
    , currency_exchange_rate
    , commission
    , selling_price * currency_exchange_rate as selling_price
  from
    `datamart-finance.staging.v_hotel_bookings`
  where
    updated_date >= (select filter2 from fd)
)
, hbd as (
  select
    cast(hbd.itinerary_id as string) as itinerary_id
    , sum(hbd.subsidy_price) as subsidy_price
    , sum(hbd.total_net_rate_price) as nett_rate_hotel
    , sum(round(hbd.total_net_rate_price * hb.currency_exchange_rate)) as cogs_native
  from  
    (
    select
      id
      , itinerary_id
      , subsidy_price
      , total_net_rate_price
    from
      `datamart-finance.staging.v_hotel_booking_details`
    where
      updated_date >= (select filter2 from fd)
    group by
      1,2,3,4
    ) hbd
    left join hb on cast(hbd.itinerary_id as string) = hb.itinerary_id
  group by
    itinerary_id
)
, hbao as (
  select 
    safe_cast(hbao.itinerary_id as string) as itinerary_id
    , category_code
    , sum(amount) as add_ons_hotel_quantity
    , sum(round(total_net_rate_price * hb.currency_exchange_rate)) as add_ons_hotel_net_price_amount
    , sum(round(total_sell_rate_price * hb.currency_exchange_rate)) as add_ons_hotel_sell_price_amount
  from 
    `datamart-finance.staging.v_hotel_booking_add_ons` hbao
    left join hb on cast(hbao.itinerary_id as string )= hb.itinerary_id
  group by
    1,2
)
, hbao_array as (
  select 
    itinerary_id
    , concat('[',string_agg(concat('{"category_code":"',category_code
    ,'"add_ons_revenue_category":"',coalesce(revenue_category_name,'Revenue Category is not defined')
    ,'"add_ons_commission_revenue_category":"',coalesce(commission_category_name,'Revenue Category is not defined')
    ,'"add_ons_hotel_quantity":"',safe_cast(add_ons_hotel_quantity as string)
    ,'"add_ons_hotel_net_price_amount":"',safe_cast(add_ons_hotel_net_price_amount as string)
    ,'"add_ons_hotel_sell_price_amount":"',safe_cast(add_ons_hotel_sell_price_amount as string)
    ,'"add_ons_hotel_commission_amount":"',safe_cast(add_ons_hotel_sell_price_amount - add_ons_hotel_net_price_amount as string)
    ) order by category_code asc),']') as add_ons_hotel_detail_json
    , array_agg(
        struct(
          category_code
          , coalesce(revenue_category_name,'Revenue Category is not defined') as add_ons_revenue_category
          , coalesce(commission_category_name,'Revenue Category is not defined')  as add_ons_commission_revenue_category
          , add_ons_hotel_quantity
          , add_ons_hotel_net_price_amount
          , add_ons_hotel_sell_price_amount
          , add_ons_hotel_sell_price_amount - add_ons_hotel_net_price_amount as add_ons_hotel_commission_amount) order by category_code asc) as add_ons_hotel_detail_array 
  from 
    hbao
    left join master_category_add_ons mcao using (category_code)
  group by 1
)
, hbao_sum as (
  select
    itinerary_id
    , sum(add_ons_hotel_net_price_amount) as total_add_ons_hotel_net_price_amount
    , sum(add_ons_hotel_sell_price_amount) as total_add_ons_hotel_sell_price_amount
    , sum(add_ons_hotel_sell_price_amount) - sum(add_ons_hotel_net_price_amount) as total_add_ons_hotel_commission_amount
  from
    hbao
  group by 
    1
)
, oar as ( --SSRR Hotel wahyu @27 Agustus 2020
  select 
    distinct(coalesce(oar.new_order_id,hr.order_id,0)) as order_id
    , coalesce(oar.order_id,hr.old_id_rebooking) as old_id_rebooking
    , coalesce(safe_cast(total_customer_price-new_total_customer_price as float64),diff_amount_rebooking,0) as diff_amount_rebooking
  from
  (
    select
      new_order_id
      , order_id
      , total_customer_price
      , new_total_customer_price
    from
      `datamart-finance.staging.v_order__automatic_rebooking`
    where
      rebook_status='SUCCESS'
  ) as oar
  full join /*EDP 01 Dec 2021, handling null rebooking data on table order__automatic_rebooking*/
  (
    select
      order_id
      , old_id_rebooking
      , diff_amount_rebooking
    from
      hcb
    where
      confirm_status = 'success'
      and old_id_rebooking <> 0
      and diff_amount_rebooking <> 0
   ) as hr on oar.new_order_id = hr.order_id
)
, fact_hotel as (
  select
    order_id
    , itinerary_id
    , quantity as quantity_hotel
    , booking_checkoutdate
    , case
        when room_source = 'TIKET' then hbd.cogs_native
        when room_source LIKE '%AGODA%' then case when ott.rebooking_price_hotel > 0 then ott.rebooking_price_hotel - round(ott.vendor_incentive_hotel*0.6) else ott.nett_price - round(ott.vendor_incentive_hotel*0.6) end/*update commission agoda(for tiket 60%,vendor 40%) */
        when room_source like 'HOTELBEDS%' then case when ott.rebooking_price_hotel > 0 then ott.rebooking_price_hotel - round(safe_divide(ott.markup_percentage_hotel*ott.rebooking_price_hotel,(100+ott.markup_percentage_hotel))) else ott.nett_price end
        else case when ott.rebooking_price_hotel > 0 then ott.rebooking_price_hotel else ott.nett_price end
      end as cogs_hotel
    , case
        when room_source = 'TIKET' then (round(coalesce(hbd.subsidy_price,0) + coalesce(ott.auto_subsidy_value),0)) 
        when room_source != 'TIKET' then (round(coalesce(ott.subsidy_price,0) + coalesce(ott.auto_subsidy_value),0)) 
      end * -1 as subsidy_hotel
    , ott.upselling_hotel
    , auto_subsidy_value
    , hbd.subsidy_price
    , ott.vendor_incentive_hotel
    , case /* 01 Jun 2020, Anggi Anggara: Request to split booking.com and AGODA */
        when room_source = 'TIKET' then hb.hotel_id
        when room_source like '%EXPEDIA%' then 'EXPEDIA'
        when room_source like '%HOTELBEDS%' then 'HOTELBEDS'
        when room_source like '%AGODA%' then 
              case 
                when room_source_info like '%BOOKING%' then 'BOOKING.COM'
                else 'AGODA'
              end
        else room_source
      end as product_provider_hotel
    , case /* 01 Jun 2020, Anggi Anggara: Request to split booking.com and AGODA */
        when ott.room_source = 'TIKET' then hb.hotel_id
        when ott.room_source != 'TIKET' then 
          case
            when room_source like '%EXPEDIA%' then 'VR-00000023'
            when room_source like '%HOTELBEDS%' then 'VR-00000024'
            when room_source like '%AGODA%' then 
              case 
                when room_source_info like '%BOOKING%' then 'VR-00015310'
                else 'VR-00000025'
              end
          end
        end as supplier_hotel
    , hotel_payment_type
    , is_hotel_issued_flag
    , rebooking_price_hotel as rebooking_price_hotel
    , add_ons_hotel_detail_json
    , add_ons_hotel_detail_array
    , coalesce(total_add_ons_hotel_net_price_amount,0) as total_add_ons_hotel_net_price_amount
    , coalesce(total_add_ons_hotel_sell_price_amount,0) as total_add_ons_hotel_sell_price_amount
    , coalesce(total_add_ons_hotel_commission_amount,0) as total_add_ons_hotel_commission_amount
    , 'Room' as revenue_category_hotel
    , case
        when mst_prop.property_category = 'nha' then 'Hotel_NHA'
        else 'Hotel'
      end as product_category_hotel
    , hb.commission as commission_percentage
    , hb.selling_price
    , case
        when room_source_info = 'Tiket Network Pte Ltd' then 'SGP'
        else 'IDN'
      end as intercompany_info
    , concat /* EDP 01 Des 2021, to identify sales entity IDN/SGP */
      (
        '['
        , case
            when room_source_info = 'Tiket Network Pte Ltd' then '{"company":"IDN","pair_company":"SGP"}'
            else '{"company":"SGP","pair_company":"IDN"}' end
        , ']'
      ) as intercompany_json
  from
    ott
    left join hb using (itinerary_id)
    left join hbd using (itinerary_id)
    left join hbao_array using (itinerary_id)
    left join hbao_sum using (itinerary_id)
    left join mst_prop on ott.hotel_id_oth = mst_prop.public_id
)
/* Fact Airport Trasfer */
, apt as (
  select 
    od.orderDetailId as order_detail_id
    , tr.totalunit as quantity_airport_transfer
    , tr.totalPrice as cogs_airport_transfer
    , UPPER(fleets.businessName) as old_supplier_id
    , UPPER(fleets.businessName) as old_product_provider_id
    , UPPER(pricingZoneName) as zone_airport_transfer
  from 
    (
      select 
        orderdata.orderdetail
        , bookingdata.totalunit
        , bookingdata.price
        , bookingdata.totalPrice 
        , checkoutdata.fleets
        , row_number() over(partition by _id order by updateddate desc) as rn 
      from 
        `datamart-finance.staging.v_apt_order__apt_order`
    ) tr
  , unnest (orderdetail) od with offset off_od
  , unnest (fleets) fleets with offset off_fleets
  , unnest(fleets.pricingZones) zone with offset off_zone
  where 
    rn = 1 
    and off_od = off_fleets
)
, fact_airport_transfer as (
  select
    safe_cast(order_detail_id as int64) as order_detail_id
    , quantity_airport_transfer
    , cogs_airport_transfer
    , old_supplier_id
    , old_product_provider_id
    , zone_airport_transfer
    , 'Shuttle' as revenue_category_airport_transfer
    , 'Car' as product_category_airport_transfer
  from
    apt
)
, combine as (
  select
    o.order_id
    , o.order_detail_id
    , case
          when date(payment_timestamp) >= '2021-03-01' 
          and reseller_id in (34382690,34423384,34433582) /*24 March 21, add new business id Agoda non login (34423384) , 1 Nov 2021 add rakuten as a customer 34433582- EDP*/
        then 'GTN_SGP'
      else 'GTN_IDN' end as company
    , case
          when date(payment_timestamp) >= '2020-04-06' and o.corporate_flag is not null and o.payment_source  in ('cash_onsite','corporate_deposit','corporate_invoice','corporate_billing') then o.business_id /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when date(payment_timestamp) <'2020-04-06' and o.corporate_flag is not null then o.business_id /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/ /*2021=07-18 - adding new payment source for b2b corporate*/
          when 
            date(payment_timestamp) >= '2020-04-06' and o.reseller_type in ('none','online_marketing','native_apps')
            and (o.corporate_flag is null or (o.corporate_flag is not null and o.payment_source not in ('cash_onsite','corporate_deposit','corporate_invoice','corporate_billing')))
              then safe_cast(o.account_id as string)
          when 
            date(payment_timestamp) < '2020-04-06' and o.reseller_type in ('none','online_marketing','native_apps')
            and (o.corporate_flag is null)
              then safe_cast(o.account_id as string)
          when o.reseller_type in ('none','online_marketing','native_apps') then safe_cast(o.account_id as string)
          when o.reseller_type in ('tiket_agent','txtravel','agent','affiliate') then safe_cast(o.reseller_id as string)
          when o.reseller_type in ('reseller','widget') then safe_cast(o.reseller_id as string)
        end as customer_id
    , case
          when date(payment_timestamp) >= '2020-04-06' and o.corporate_flag is not null and o.payment_source  in ('cash_onsite','corporate_deposit','corporate_invoice','corporate_billing') then 'B2B Corporate' /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when date(payment_timestamp) <'2020-04-06' and o.corporate_flag is not null then 'B2B Corporate' /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when 
            date(payment_timestamp) >= '2020-04-06' and o.reseller_type in ('none','online_marketing','native_apps')
            and (o.corporate_flag is null or (o.corporate_flag is not null and o.payment_source not in ('cash_onsite','corporate_deposit','corporate_invoice','corporate_billing')))
              then 'B2C'
          when 
            date(payment_timestamp) < '2020-04-06' and o.reseller_type in ('none','online_marketing','native_apps')
            and (o.corporate_flag is null)
              then 'B2C'
          when o.reseller_type in ('none','online_marketing','native_apps') then 'B2C'
          when o.reseller_type in ('tiket_agent','txtravel','agent','affiliate') then 'B2B Offline'
          when o.reseller_type in ('reseller','widget') then 'B2B Online'
        end as customer_type
    , o.selling_currency as selling_currency
    , o.payment_timestamp
    , date(o.payment_timestamp) as payment_date
    , o.order_type as order_type
    , o.auth_code as authentication_code
    , o.virtual_account
    , case
        when o.order_type in ('tixhotel','hotel','flight','car','train','event','airport_transfer','railink') then trim(o.giftcard_voucher)
        else null
      end as giftcard_voucher
    , case
        when o.order_type in ('tixhotel','hotel','flight','car','train','event','airport_transfer','railink') then trim(o.promocode_name)
        else null
      end as promocode_name
    , o.payment_source as payment_source
    , o.payment_gateway as payment_gateway
    , o.payment_type_bank as payment_type_bank
    , o.payment_order_name as payment_order_name
    , coalesce(
        round(                                                  /* there are cases that if we only use round(payment_charge*selling proportion) then when it summed by order_id */
          case                                                  /* , the summed value will be more than the original value. */
            when o.order_detail_id = o.last_order_detail_id /* For example, if there are 2 order_detail_id and the proportion is 0.5 and 0.5, the total is 301, */
              then o.payment_charge - sum(                   /* then it will be 151 for each order detail id. When summed by order_id, it will give result 302. */ 
                                          round(                /* Hence, we calculate for the last order_detail_id will be the remain */ 
                                            case                /* of original price minus sum(round(payment_charge * selling_proportion)) of other order_detail_id  */
                                              when o.order_detail_id != o.last_order_detail_id then o.payment_charge 
                                              else 0 
                                            end * o.selling_price_proportion_value)) over(partition by order_id)
            else o.payment_charge * o.selling_price_proportion_value
          end
        ,0)
      ,0) as payment_charge
    , coalesce(round( 
        case 
          when o.order_detail_id = o.last_order_detail_id 
            then o.convenience_fee_amount - sum(
                                            round(
                                              case 
                                                when o.order_detail_id != o.last_order_detail_id then o.convenience_fee_amount 
                                                else 0 
                                              end * o.selling_price_proportion_value)) over(partition by order_id)
          else o.convenience_fee_amount * o.selling_price_proportion_value
         end
      ,0),0) as convenience_fee_amount
    , coalesce(round( 
        case 
          when o.order_detail_id = o.last_order_detail_id 
            then o.promocode_value - sum(
                                          round(
                                            case 
                                              when o.order_detail_id != o.last_order_detail_id then o.promocode_value 
                                              else 0 
                                            end * o.selling_price_proportion_value)) over(partition by order_id)
          else o.promocode_value * o.selling_price_proportion_value
         end
      ,0),0) as promocode_value
    , coalesce(round( 
        case 
          when o.order_detail_id = o.last_order_detail_id 
            then o.giftvoucher_value - sum(
                                            round(
                                              case 
                                                when o.order_detail_id != o.last_order_detail_id then o.giftvoucher_value 
                                                else 0 
                                              end * o.selling_price_proportion_value)) over(partition by order_id)
          else o.giftvoucher_value * o.selling_price_proportion_value
         end
      ,0),0) as giftvoucher_value
    , coalesce(round( 
        case 
          when o.order_detail_id = o.last_order_detail_id 
            then o.refund_deposit_value - sum(
                                                round(
                                                  case 
                                                    when o.order_detail_id != o.last_order_detail_id then o.refund_deposit_value 
                                                    else 0 
                                                  end * o.selling_price_proportion_value)) over(partition by order_id)
          else o.refund_deposit_value * o.selling_price_proportion_value
         end
      ,0),0) as refund_deposit_value
    , coalesce(round( 
        case 
          when o.order_detail_id = o.last_order_detail_id 
            then o.tiketpoint_value - sum(
                                            round(
                                              case 
                                                when o.order_detail_id != o.last_order_detail_id then o.tiketpoint_value 
                                                else 0 
                                              end * o.selling_price_proportion_value)) over(partition by order_id)
          else o.tiketpoint_value * o.selling_price_proportion_value
         end
      ,0),0) as tiketpoint_value
    , coalesce(ifnull(o.insurance_value,0)+ifnull(o.premium,0),0) as insurance_value
    , concat
      (
        case
          when o.insurance_json is not null
            or o.flight_insurance_json is not null
            then '[' else null
          end
        , coalesce(o.flight_insurance_json,'')
        , case
            when o.insurance_json is not null and o.flight_insurance_json is not null
            then ',' else ''
          end
        , case when o.insurance_json is not null then o.insurance_json else '' end
        , case
          when o.insurance_json is not null
            or o.flight_insurance_json is not null
            then ']' else null
          end
      )
      as flight_insurance_json
    , coalesce(o.cancel_insurance_value,0) as cancel_insurance_value
    , o.giftvoucher_name as giftvoucher_name
    , o.giftcard_voucher_purpose
    , o.giftcard_voucher_user_email_reference_id
    , o.refund_deposit_name
    , concat
      (
        coalesce(o.insurance_name,'')
        , case
            when o.insurance_package is not null and o.insurance_name is not null
            then ',' else ''
          end
        , case when o.insurance_package is not null then o.insurance_package else '' end
      )
      as insurance_name
    , o.cancel_insurance_name
    , o.nominal_value
    , o.percentage_value
    , case
        when 
          oar.old_id_rebooking is not null and oar.diff_amount_rebooking > 0 
            and (o.promocode_value is null or o.promocode_value = 0)
          then safe_cast(o.payment_amount+ifnull(oar.diff_amount_rebooking,0) as numeric)
        else o.payment_amount
      end as payment_amount
    , round(
        coalesce( 
          case 
            when o.order_detail_id = o.last_order_detail_id 
              then (o.nominal_value + (o.percentage_value*o.payment_amount/100)) - sum(
                                              round(
                                                case 
                                                  when o.order_detail_id != o.last_order_detail_id then (o.nominal_value + (o.percentage_value*o.payment_amount/100)) 
                                                  else 0 
                                                end * o.selling_price_proportion_value)) over(partition by order_id)
            else (o.nominal_value + (o.percentage_value*o.payment_amount/100)) * o.selling_price_proportion_value
           end
        ,0)
      ,2) as pg_charge
    , o.cc_installment
    , o.order_name
    , o.order_name_detail
    , o.insurance_issue_code as insurance_issue_code
    , o.cancel_insurance_issue_code
    , o.acquiring_bank
    , coalesce(
        ft.cogs_train
        , fr.cogs_railink
        , case
            when o.order_type = 'car' then o.selling_price - total_addons_car
            else null
          end
        , case
            when o.order_type = 'event' then o.selling_price - fttd.commission_event - subsidy_event - upselling_event
            else null
          end
        , fh.cogs_hotel-ifnull(premium,0)
        , case 
            when date(o.payment_timestamp) >= '2021-10-15' and order_flight_commission > 0 and ff.supplier_flight = 'VR-00000011' and o.is_reschedule is null then ff.cogs_flight - order_flight_commission /* 15 Oct 2021, breakdown commission for trigana only */
            when date(o.payment_timestamp) >= '2020-05-11' and ff.supplier_flight in ('VR-00000006','VR-00000011','VR-00000004','VR-00017129','VR-00000002') then ff.cogs_price_nta_flight 
/* 13 May 2020, Anggi Anggara: for lion group, start order >= 2020-05-11 using price_nta */
/* 27 May 2020, Anggi Anggara: for trigana, sriwjaya , transnusa, start order >= 2020-05-11 using price_nta */
/* 04 Oct 2021, add garuda (since garuda has a commission from price_nta) */
            when date(o.payment_timestamp) >= '2021-02-20' and order_flight_commission > 0 and ff.supplier_flight in ('VR-00000003') and manual_markup_amount > 0 then ff.cogs_flight /* 20 feb 2021, for sabre with markup amount > 0 */
            when date(o.payment_timestamp) >= '2020-10-01' and order_flight_commission > 0 and ff.supplier_flight in ('VR-00000003','VR-00000007','VR-00000012') and o.is_reschedule is null then ff.cogs_flight - order_flight_commission /* 1 oct 2020, for sabre, transnusa, express only */
            else ff.cogs_flight
          end
        , fat.cogs_airport_transfer
        , 0
      ) as cogs
    , coalesce(
        ft.quantity_train
        , fr.quantity_railink
        , fc.quantity_car
        , fttd.quantity_event
        , ff.quantity_flight
        , fh.quantity_hotel
        , fat.quantity_airport_transfer
        , 0
      ) as quantity
    , coalesce(
        ft.commission_train
        , fr.commission_railink
        , fc.commission_car
        , fttd.commission_event
        , case
            when date(o.payment_timestamp) < '2021-10-01' and o.order_type = 'tixhotel' and o.reseller_type in ('reseller','widget') and reseller_id = 34272813
              then round(fh.commission_percentage*fh.selling_price/100) /* 10 MARCH 2021, breakdown partner commission for CTRIP*/
            when date(o.payment_timestamp) >= '2021-10-01' and o.order_type = 'tixhotel' and o.reseller_type in ('reseller','widget') 
              then round(fh.commission_percentage*fh.selling_price/100) /* 01 Oct 2021, breakdown partner commission for all b2b online */
            when o.order_type = 'tixhotel' then
              case when fh.rebooking_price_hotel > 0 then fh.rebooking_price_hotel - fh.cogs_hotel + (fh.subsidy_hotel*-1) - fh.upselling_hotel - fh.total_add_ons_hotel_sell_price_amount
              else o.selling_price - fh.cogs_hotel + (fh.subsidy_hotel*-1) - fh.upselling_hotel - fh.total_add_ons_hotel_sell_price_amount
              end
          end
        , case 
            when date(o.payment_timestamp) >= '2021-10-15' and order_flight_commission > 0 and ff.supplier_flight = 'VR-00000011' and o.is_reschedule is null then order_flight_commission /* 15 Oct 2021, breakdown commission for trigana only */
            when date(o.payment_timestamp) >= '2020-05-11' and ff.supplier_flight in ('VR-00000006','VR-00000011','VR-00000004','VR-00017129','VR-00000002') then ff.commission_price_nta_flight 
/* 13 May 2020, Anggi Anggara: for lion group, start order >= 2020-05-11 using price_nta */
/* 27 May 2020, Anggi Anggara: for trigana, sriwjaya , transnusa, start order >= 2020-05-11 using price_nta */
/* 04 Oct 2021, add garuda (since garuda has a commission from price_nta) */
             - safe_cast(fmd.manual_markup_amount as numeric)
             when date(o.payment_timestamp) >= '2021-02-20' and order_flight_commission > 0 and ff.supplier_flight in ('VR-00000003') and manual_markup_amount > 0 then 0 /*commission_flight_fmd*/ /* 20 feb 2021, for sabre with markup amount > 0 , 27 dec 2021 updated 0 to handling mismatch data (commission=upselling)*/
            when date(o.payment_timestamp) >= '2020-10-01' and order_flight_commission > 0 and ff.supplier_flight in ('VR-00000003','VR-00000007','VR-00000012') and o.is_reschedule is null then order_flight_commission /* 1 oct 2020, for sabre, transnusa, express only */
            when ff.commission_flight = 0 and fmd.manual_markup_amount > 0 then 0 /*04 oct 2021, handling mismatch for hardcoded commission = upselling*/
            when ff.commission_flight = ff.subsidy_discount then 0 /*14 oct 2021, qatar airways has same commission & subsidy (gbv has reduced by subsidy*/
            else ff.commission_flight
             - safe_cast(fmd.manual_markup_amount as numeric)
          end
        , 0
      ) as commission
    , coalesce(
        case
          when date(o.payment_timestamp) < '2021-10-01' and o.order_type = 'tixhotel' and o.reseller_type in ('reseller','widget') and reseller_id = 34272813
            then -1*round(fh.cogs_hotel+(fh.commission_percentage*fh.selling_price/100)-o.customer_price+fh.subsidy_hotel+fh.upselling_hotel)
          when date(o.payment_timestamp) >= '2021-10-01' and o.order_type = 'tixhotel' and o.reseller_type in ('reseller','widget') /* 01 Oct 2021, breakdown partner commission for all b2b online */
            then -1*round(fh.cogs_hotel+(fh.commission_percentage*fh.selling_price/100)-o.customer_price+fh.subsidy_hotel+fh.upselling_hotel)
            else 0
          end
        , 0
      ) as partner_commission  
    , coalesce(
        ft.upselling_train
        , fr.upselling_railink
        , fc.upselling_car
        , fttd.upselling_event
        , case
            when ff.supplier_flight not in ('VR-00000002','VR-00000005')
              then safe_cast(fmd.manual_markup_amount as numeric)
            when ff.supplier_flight in ('VR-00000005') and ff.upselling_flight <> 0 and ff.upselling_flight = ff.subsidy_discount
              then 0 /* EDP. temporarily per Nov 2021 citilink hasn't upselling again, and per Dec 17 2021 amount of upselling = subsidy so the uppselling updated to 0*/  
            else ff.upselling_flight
          end
        , fh.upselling_hotel
        , 0
      ) as upselling
    , coalesce(
        ft.subsidy_train
        , fr.subsidy_railink
        , fc.subsidy_car
        , fttd.subsidy_event
        , case 
            when ff.subsidy_flight <> 0 then ff.subsidy_flight
            when ff.subsidy_discount <> 0 then subsidy_discount
            else null
          end  
        , fh.subsidy_hotel
        , 0
      ) as subsidy
    , ff.subsidy_category
    , coalesce(
        ft.product_category_train
        , fr.product_category_railink
        , fc.product_category_car
        , fttd.product_category_event
        , ff.product_category_flight
        , fh.product_category_hotel
        , fat.product_category_airport_transfer
      ) as product_category
    , fh.booking_checkoutdate as hotel_checkoutdate
    , coalesce(fh.rebooking_price_hotel,0) as rebooking_price_hotel
    , case
        when fh.rebooking_price_hotel>0 then o.selling_price - fh.rebooking_price_hotel
        else 0
      end as rebooking_sales_hotel
    , coalesce(
        ft.supplier_train
        , fr.supplier_railink
        , fc.supplier_car
        , coalesce(mes.new_supplier_id,fttd.supplier_event) /* include for airport transfer because in one join with master event */
        , ff.supplier_flight
        , case
            when o.reseller_type in ('reseller','widget')
              and date(payment_timestamp) >= '2021-03-01' 
              and reseller_id in (34382690,34423384,34433582) /*24 March 21, add new business id Agoda non login (34423384), 1 Nov 2021 add rakuten as a customer 34433582- EDP*/
              and intercompany_info = 'IDN'
            then 'VR-00014887'
            else fh.supplier_hotel
          end
      ) as supplier
    , coalesce(
        ft.product_provider_train
        , fr.product_provider_railink
        , fc.product_provider_car
        , coalesce(mepp.new_product_provider_id,fttd.product_provider_event)  /* include for airport transfer because in one join with master event */
        , case 
            when o.order_type = 'flight' then safe_cast(o.order_master_id as string)
            else null 
          end
        , fh.product_provider_hotel
      ) as product_provider
    , coalesce(
        ft.revenue_category_train
        , fr.revenue_category_railink
        , fc.revenue_category_car
        , fttd.revenue_category_event
        , ff.revenue_category_flight
        , fh.revenue_category_hotel
        , fat.revenue_category_airport_transfer
      ) as revenue_category
    , coalesce(
        fr.vat_out_railink
        , ft.vat_out_train
        , 0
      ) as vat_out
    , coalesce(
        ff.baggage_fee
        , 0
      ) as baggage_fee
    , coalesce(
        rrbc.reschedule_fee_flight
        , 0
      ) as reschedule_fee_flight
    , coalesce(
        round( 
          case 
            when o.order_detail_id = o.last_order_detail_id 
              then rrbc.reschedule_cashback_amount - sum(
                                              round(
                                                case 
                                                  when o.order_detail_id != o.last_order_detail_id then rrbc.reschedule_cashback_amount 
                                                  else 0 
                                                end * o.selling_price_proportion_value)) over(partition by order_id)
            else rrbc.reschedule_cashback_amount * o.selling_price_proportion_value
           end
        ,0)
        , 0
      ) as reschedule_cashback_amount
    , coalesce(
        round( 
          case 
            when o.order_detail_id = o.last_order_detail_id 
              then rrbc.reschedule_promocode_amount - sum(
                                              round(
                                                case 
                                                  when o.order_detail_id != o.last_order_detail_id then rrbc.reschedule_promocode_amount 
                                                  else 0 
                                                end * o.selling_price_proportion_value)) over(partition by order_id)
            else rrbc.reschedule_promocode_amount * o.selling_price_proportion_value
           end
        ,0)
        , 0
      ) * -1 as reschedule_promocode_amount
    , flight_reschedule_old_order_detail_id
    , product_provider_reschedule_flight
    , supplier_reschedule_flight
    , coalesce(
        round( 
          case 
            when o.order_detail_id = o.last_order_detail_id 
              then rrbc.refund_amount_flight - sum(
                                              round(
                                                case 
                                                  when o.order_detail_id != o.last_order_detail_id then rrbc.refund_amount_flight 
                                                  else 0 
                                                end * o.selling_price_proportion_value)) over(partition by order_id)
            else rrbc.refund_amount_flight * o.selling_price_proportion_value
           end
        ,0)
        , 0
      ) as refund_amount_flight
    , coalesce(
        round( 
        case 
          when o.order_detail_id = o.last_order_detail_id 
            then o.refund_deposit_value - sum(
                                                round(
                                                  case 
                                                    when o.order_detail_id != o.last_order_detail_id then o.refund_deposit_value 
                                                    else 0 
                                                  end * o.selling_price_proportion_value)) over(partition by order_id)
          else o.refund_deposit_value * o.selling_price_proportion_value
         end
      ,0)
      * -1 
      - round( 
          case 
            when o.order_detail_id = o.last_order_detail_id 
              then rrbc.refund_amount_flight - sum(
                                              round(
                                                case 
                                                  when o.order_detail_id != o.last_order_detail_id then rrbc.refund_amount_flight 
                                                  else 0 
                                                end * o.selling_price_proportion_value)) over(partition by order_id)
            else rrbc.refund_amount_flight * o.selling_price_proportion_value
           end
        ,0)
      ,0) as reschedule_miscellaneous_amount
    , coalesce(
        ft.booking_code_train
        , fr.booking_code_railink
        , ff.booking_code_flight
        , fh.itinerary_id
      ) as booking_code
    , coalesce(
        fttd.ticket_number_event
        , ff.ticket_number_flight
      ) as ticket_number
    , oar.old_id_rebooking
    , coalesce(oar.diff_amount_rebooking,0) as diff_amount_rebooking
    , add_ons_hotel_detail_json
    , add_ons_hotel_detail_array as add_ons_hotel_detail_array
    , coalesce(total_add_ons_hotel_net_price_amount,0) as total_add_ons_hotel_net_price_amount
    , coalesce(total_add_ons_hotel_sell_price_amount,0) as total_add_ons_hotel_sell_price_amount
    , coalesce(total_add_ons_hotel_commission_amount,0) as total_add_ons_hotel_commission_amount
    , coalesce(halodoc_sell_price_amount,0) as halodoc_sell_price_amount
    , coalesce(halodoc_pax_count,0) as halodoc_pax_count
    , case 
        when o.order_type in ('flight','train','railink') then concat(safe_cast(o.order_id as string), ' - ', o.order_name_detail, ' / ', o.order_name)
        when o.order_type in ('tixhotel','event') then concat(safe_cast(o.order_id as string), ' - ', o.order_name, ' / ', o.order_name_detail)
        when o.order_type in ('car') then concat(safe_cast(o.order_id as string), ' - ', o.order_name_detail)
        when order_type = 'airport_transfer' then concat(safe_cast(o.order_id as string), ' - ', order_name_detail, ' - ', zone_airport_transfer)
      end as memo_product
    , case 
        when o.order_type in ('tixhotel') then concat(safe_cast(o.order_id as string), ' - ', o.order_name, ' / ', o.order_name_detail, ' - ', fh.itinerary_id)
      end as memo_hotel
    , case 
        when o.order_type in ('flight') then concat(safe_cast(o.order_id as string), ' / ', ff.booking_code_flight, ' - ', o.order_name_detail, ' / ', o.order_name)
      end as memo_flight
    , case
        when cancel_insurance_value is not null then concat(safe_cast(o.order_id as string), ' - ', 'Cermati Anti Galau, Issue Code: ', ifnull(o.cancel_insurance_issue_code,'')) 
        else null
      end as memo_cancel_insurance
    , safe_cast(null as string) as memo_insurance
    , case 
        when is_has_halodoc_flag = 1 then concat(safe_cast(o.order_id as string), ' - ', json_extract_scalar(addons_flight_json, '$[0].desc'))
        else null
      end as memo_halodoc
    , addons_flight_json
    , case 
        when convenience_fee_amount > 0 then concat(safe_cast(o.order_id as string), ' - ', convenience_fee_order_name)
        else null
      end as memo_convenience_fee
    , case 
        when giftvoucher_value < 0 then 
          case
            when giftcard_voucher_purpose = 'REFUND_COVID' and giftcard_voucher_user_email_reference_id is not null then concat(safe_cast(order_id as string),' - ', giftcard_voucher_user_email_reference_id)
            else concat(safe_cast(order_id as string),' - ', giftcard_voucher)
          end
        else null
      end as memo_giftvoucher
    , case
        when o.order_type not in ('flight','train','tixhotel','railink') then 'issued'
        when o.order_type in ('flight','train','railink') and coalesce(ff.ticket_status_flight, ft.ticket_status_train, fr.ticket_status_railink) = 'issued' then 'issued'
        when o.order_type in ('tixhotel') and fh.is_hotel_issued_flag = 1 then 'issued'
        else 'not issued'
      end as issued_status
    , o.selling_price_proportion_value
    , case 
        when 
          string_agg(case
            when o.order_type not in ('flight','train','tixhotel','railink') then 'issued'
            when o.order_type in ('flight','train','railink') and coalesce(ff.ticket_status_flight, ft.ticket_status_train, fr.ticket_status_railink) = 'issued' then 'issued'
            when o.order_type in ('tixhotel') and fh.is_hotel_issued_flag = 1 then 'issued'
            else 'not issued'
          end) over(partition by order_id) like '%not_issued%' 
        then 0
        else 1
      end as all_issued_flag
    , case
        when 
          o.order_type = 'event' 
          and 
            (
              (
                coalesce(mepp.new_product_provider_id,fttd.product_provider_event) = '(null)'
                or safe_cast(coalesce(mepp.new_product_provider_id,fttd.product_provider_event) as string) = '0' 
                or safe_cast(coalesce(mepp.new_product_provider_id,fttd.product_provider_event) as string) = '-'
                or safe_cast(coalesce(mepp.new_product_provider_id,fttd.product_provider_event) as string) = '(blank)'
                or coalesce(mes.new_supplier_id,fttd.supplier_event) = '(null)'
                or safe_cast(coalesce(mes.new_supplier_id,fttd.supplier_event) as string) = '0' 
                or safe_cast(coalesce(mes.new_supplier_id,fttd.supplier_event) as string) = '-'
                or safe_cast(coalesce(mes.new_supplier_id,fttd.supplier_event) as string) = '(blank)'
              )
            or
              (
                date(o.payment_timestamp) = mes.start_date
                or date(o.payment_timestamp) = mepp.start_date
              )
            )
        then 1
        when 
          o.order_type = 'airport_transfer'
          and (mes.new_supplier_id is null or mepp.new_product_provider_id is null)
        then 1
        else 0
      end as event_data_error_flag
    , case
        when o.order_type = 'tixhotel' and fh.hotel_payment_type like '%pay_at_hotel%' then 1
        else 0
      end as pay_at_hotel_flag
    , case 
        when add_ons_hotel_detail_array is not null then 1
        else 0
      end as is_have_add_ons_hotel_flag
    , coalesce(is_has_halodoc_flag, 0) as is_has_halodoc_flag
    , case
        when oar.old_id_rebooking is null then 0
        else 1 
        end as is_rebooking_flag
    , case
        when o.is_reschedule = 1 and tfrro.is_flexi then true
        else false
        end as is_flexi_reschedule
    , safe_cast(coalesce(tfrro.fare_diff+tfrro.tax_diff+tfrro.reschedule_fee, 0) as float64) flexi_fare_diff
    , safe_cast(0 as float64) flexi_reschedule_fee --bypass increasing flexi fare diff
    , coalesce(total_ancillary_flight,0) as total_ancillary_flight
    , ancillary_flight_json
    , addons_car_json
    , coalesce(total_addons_car,0) as total_addons_car
    , case /* EDP 01 Des 2021, implement intercompany journal integrations*/
        when reseller_id in (34382690,34423384,34433582) and intercompany_info = 'IDN' then intercompany_json
        when reseller_id not in (34382690,34423384,34433582) and intercompany_info = 'SGP' and intercompany_json is not null then intercompany_json
        else null
      end as intercompany_json
  from
    `datamart-finance.datasource_workday.temp_customer_invoice_raw_part_1` o
    left join fact_train ft using (order_detail_id)
    left join fact_railink fr using (order_detail_id)
    left join fact_car fc using (order_detail_id)
    left join fact_ttd fttd using (order_id)
    left join fact_flight ff using (order_detail_id)
    left join fact_hotel fh using (order_id)
    left join fact_airport_transfer fat using (order_detail_id)
    left join ores using (order_id)
    left join tfrro on o.order_detail_id = tfrro.new_order_detail_id
    left join rrbc using (order_id, reschedule_passenger_id)
    left join ocf_reschedule using (flight_reschedule_old_order_detail_id)
    left join master_event_supplier mes on (coalesce(fttd.supplier_event,fat.old_supplier_id) = mes.old_supplier_id and o.order_name = mes.event_name)
    left join master_event_product_provider mepp on (coalesce(fttd.product_provider_event,fat.old_product_provider_id) = mepp.old_product_provider_id and o.order_name = mepp.event_name)
    left join oar using (order_id)
    left join fmd  using (order_detail_id)
)
, fact as (
  select
    c.*
    , case
        when c.product_category not in ('Flight','Train') 
            and (
              (c.product_category in ('Event','Activity','Attraction') and c.event_data_error_flag != 1)  
              or (c.product_category in ('Hotel') and c.product_provider not in ('EXPEDIA','AGODA','HOTELBEDS','BOOKING.COM')) 
              or (c.product_category in ('Car') and revenue_category not in ('Shuttle'))
            ) then 
          case
            when ms.active_date >= c.payment_date or ms.active_date is null then 1
            else 0
          end
        else 0
      end as new_supplier_flag
    , case
        when c.product_category not in ('Flight','Train') 
            and (
              (c.product_category in ('Event','Activity','Attraction') and c.event_data_error_flag != 1)  
              or (c.product_category in ('Hotel') and c.product_provider not in ('EXPEDIA','AGODA','HOTELBEDS','BOOKING.COM')) 
              or (c.product_category in ('Car') and revenue_category not in ('Shuttle'))
            ) then 
          case
            when mpp.active_date >= c.payment_date or mpp.active_date is null then 1
            else 0
          end
        else 0
      end as new_product_provider_flag
    , case
        when c.customer_type in ('B2B Online','B2B Offline') then
          case
            when mco.active_date >= c.payment_date or mco.active_date is null then 1
            else 0
          end
        else 0
      end as new_b2b_online_and_offline_flag
    , case
        when c.customer_type = 'B2B Corporate' then
          case
            when mcc.active_date >= c.payment_date or mcc.active_date is null then 1
            else 0
          end
        else 0
      end as new_b2b_corporate_flag
    , case
        when c.product_category = 'Flight' and sum(case when c.supplier is null then 1 else 0 end) over(partition by order_id) > 0 then 1
        else 0
      end as is_supplier_flight_not_found_flag
    , case 
        when payment_source not in('tiketpoint','giftcard')
        and sum(cogs + commission + partner_commission + upselling + subsidy + payment_charge + promocode_value + giftvoucher_value + refund_deposit_value + tiketpoint_value + insurance_value + cancel_insurance_value + ifnull(vat_out,0) + ifnull(total_ancillary_flight,0) + rebooking_sales_hotel + total_add_ons_hotel_sell_price_amount + halodoc_sell_price_amount + convenience_fee_amount + diff_amount_rebooking + ifnull(total_addons_car,0)) over(partition by order_id) <> payment_amount
          then 0
        else 1
      end as is_amount_valid_flag
  from
    combine as c
    left join master_data_supplier ms on ms.Supplier_Reference_ID = c.supplier and c.payment_date >= ms.start_date and c.payment_date < ms.end_date
    left join master_data_product_provider mpp on mpp.Organization_Reference_ID = c.product_provider and c.payment_date >= mpp.start_date and c.payment_date < mpp.end_date
    left join master_data_customer mco on mco.Customer_Reference_ID = c.customer_id and mco.Customer_Category_ID in('B2B Online','B2B_Online','B2B Offline') and c.payment_date >= mco.start_date and c.payment_date < mco.end_date
    left join master_data_customer mcc on mcc.Customer_Reference_ID = c.customer_id and mcc.Customer_Category_ID = 'B2B_Corporate' and c.payment_date >= mcc.start_date and c.payment_date < mcc.end_date 
)
, tr as (
  select 
    * except (rn)
  from
    (
      select
        *
        , row_number() over(partition by order_id, order_detail_id order by processed_timestamp desc) as rn
      from
        `datamart-finance.datasource_workday.customer_invoice_raw`
      where payment_date >= (select date(filter1,'Asia/Jakarta') from fd)
    )
  where rn = 1
)
, append as (
  select
    fact.*
    , timestamp(datetime(current_timestamp(),'Asia/Jakarta')) as processed_timestamp
  from
    fact
  left join tr on
  (tr.order_id = fact.order_id)	
  and (tr.order_detail_id = fact.order_detail_id)	
  and (tr.company = fact.company)	
  and (tr.customer_id = fact.customer_id or (tr.customer_id is null and fact.customer_id is null))	
  and (tr.customer_type = fact.customer_type)	
  and (tr.selling_currency = fact.selling_currency 	)
  and (tr.payment_timestamp = fact.payment_timestamp 	)
  and (tr.payment_date = fact.payment_date 	)
  and (tr.order_type = fact.order_type 	)
  and (tr.authentication_code = fact.authentication_code 	or (tr.authentication_code is null and fact.authentication_code is null))
  and (tr.virtual_account = fact.virtual_account 	or (tr.virtual_account is null and fact.virtual_account is null))
  and (tr.giftcard_voucher = fact.giftcard_voucher 	or (tr.giftcard_voucher is null and fact.giftcard_voucher is null))
  and (tr.promocode_name = fact.promocode_name 	or (tr.promocode_name is null and fact.promocode_name is null))
  and (tr.payment_source = fact.payment_source 	or (tr.payment_source is null and fact.payment_source is null))
  and (tr.payment_gateway = fact.payment_gateway 	or (tr.payment_gateway is null and fact.payment_gateway is null))
  and (tr.payment_type_bank = fact.payment_type_bank 	or (tr.payment_type_bank is null and fact.payment_type_bank is null))
  and (tr.payment_order_name = fact.payment_order_name 	or (tr.payment_order_name is null and fact.payment_order_name is null))
  and (tr.payment_charge = fact.payment_charge 	or (tr.payment_charge is null and fact.payment_charge is null))
  and (tr.promocode_value = fact.promocode_value 	or (tr.promocode_value is null and fact.promocode_value is null))
  and (tr.giftvoucher_value = fact.giftvoucher_value 	or (tr.giftvoucher_value is null and fact.giftvoucher_value is null))
  and (tr.refund_deposit_value = fact.refund_deposit_value 	or (tr.refund_deposit_value is null and fact.refund_deposit_value is null))
  and (tr.tiketpoint_value = fact.tiketpoint_value 	or (tr.tiketpoint_value is null and fact.tiketpoint_value is null))
  and (tr.insurance_value = fact.insurance_value 	or (tr.insurance_value is null and fact.insurance_value is null))
  and (tr.cancel_insurance_value = fact.cancel_insurance_value 	or (tr.cancel_insurance_value is null and fact.cancel_insurance_value is null))
  and (tr.giftvoucher_name = fact.giftvoucher_name 	or (tr.giftvoucher_name is null and fact.giftvoucher_name is null))
  and (tr.refund_deposit_name = fact.refund_deposit_name 	or (tr.refund_deposit_name is null and fact.refund_deposit_name is null))
  and (tr.insurance_name = fact.insurance_name 	or (tr.insurance_name is null and fact.insurance_name is null))
  and (tr.cancel_insurance_name = fact.cancel_insurance_name 	or (tr.cancel_insurance_name is null and fact.cancel_insurance_name is null))
  and (tr.pg_charge = fact.pg_charge 	or (tr.pg_charge is null and fact.pg_charge is null))
  and (tr.cc_installment = fact.cc_installment 	or (tr.cc_installment is null and fact.cc_installment is null))
  and (tr.order_name = fact.order_name 	)
  and (tr.order_name_detail = fact.order_name_detail 	)
  and (tr.insurance_issue_code = fact.insurance_issue_code 	or (tr.insurance_issue_code is null and fact.insurance_issue_code is null))
  and (tr.cancel_insurance_issue_code = fact.cancel_insurance_issue_code 	or (tr.cancel_insurance_issue_code is null and fact.cancel_insurance_issue_code is null))
  and (tr.acquiring_bank = fact.acquiring_bank 	or (tr.acquiring_bank is null and fact.acquiring_bank is null))
  and (tr.cogs = fact.cogs 	or (tr.cogs is null and fact.cogs is null))
  and (tr.quantity = fact.quantity 	or (tr.quantity is null and fact.quantity is null))
  and (tr.commission = fact.commission 	or (tr.commission is null and fact.commission is null))
  and (tr.upselling = fact.upselling 	or (tr.upselling is null and fact.upselling is null))
  and (tr.subsidy = fact.subsidy 	or (tr.subsidy is null and fact.subsidy is null))
  and (tr.product_category = fact.product_category 	or (tr.product_category is null and fact.product_category is null))
  and (tr.rebooking_price_hotel = fact.rebooking_price_hotel or (tr.rebooking_price_hotel is null and fact.rebooking_price_hotel is null))
  and (tr.rebooking_sales_hotel = fact.rebooking_sales_hotel or (tr.rebooking_sales_hotel is null and fact.rebooking_sales_hotel is null))
  and (tr.supplier = fact.supplier 	or (tr.supplier is null and fact.supplier is null))
  and (tr.product_provider = fact.product_provider 	or (tr.product_provider is null and fact.product_provider is null))
  and (tr.revenue_category = fact.revenue_category 	)
  and (tr.vat_out = fact.vat_out 	or (tr.vat_out is null and fact.vat_out is null))
  and (tr.baggage_fee = fact.baggage_fee 	or (tr.baggage_fee is null and fact.baggage_fee is null))
  and (tr.flight_reschedule_old_order_detail_id = fact.flight_reschedule_old_order_detail_id 	or (tr.flight_reschedule_old_order_detail_id is null and fact.flight_reschedule_old_order_detail_id is null))
  and (tr.product_provider_reschedule_flight = fact.product_provider_reschedule_flight 	or (tr.product_provider_reschedule_flight is null and fact.product_provider_reschedule_flight is null))
  and (tr.supplier_reschedule_flight = fact.supplier_reschedule_flight 	or (tr.supplier_reschedule_flight is null and fact.supplier_reschedule_flight is null))
  and (tr.reschedule_fee_flight = fact.reschedule_fee_flight )
  and (tr.reschedule_cashback_amount = fact.reschedule_cashback_amount 	)
  and (tr.reschedule_promocode_amount = fact.reschedule_promocode_amount 	)
  and (tr.refund_amount_flight = fact.refund_amount_flight 	)
  and (tr.reschedule_miscellaneous_amount = fact.reschedule_miscellaneous_amount 	)
  and (tr.booking_code = fact.booking_code 	or (tr.booking_code is null and fact.booking_code is null))
  and (tr.ticket_number = fact.ticket_number 	or (tr.ticket_number is null and fact.ticket_number is null))
  and (tr.memo_product = fact.memo_product 	or (tr.memo_product is null and fact.memo_product is null))
  and (tr.memo_hotel = fact.memo_hotel 	or (tr.memo_hotel is null and fact.memo_hotel is null))
  and (tr.memo_flight = fact.memo_flight 	or (tr.memo_flight is null and fact.memo_flight is null))
  and (tr.memo_cancel_insurance = fact.memo_cancel_insurance 	or (tr.memo_cancel_insurance is null and fact.memo_cancel_insurance is null))
  and (tr.memo_insurance = fact.memo_insurance 	or (tr.memo_insurance is null and fact.memo_insurance is null))
  and (tr.issued_status = fact.issued_status 	or (tr.issued_status is null and fact.issued_status is null))
  and (tr.selling_price_proportion_value = fact.selling_price_proportion_value 	or (tr.selling_price_proportion_value is null and fact.selling_price_proportion_value is null))
  and (tr.all_issued_flag = fact.all_issued_flag 	)
  and (tr.event_data_error_flag = fact.event_data_error_flag 	)
  and (tr.pay_at_hotel_flag = fact.pay_at_hotel_flag 	)
  and (tr.new_supplier_flag = fact.new_supplier_flag 	)
  and (tr.new_product_provider_flag = fact.new_product_provider_flag 	)
  and (tr.new_b2b_online_and_offline_flag = fact.new_b2b_online_and_offline_flag 	)
  and (tr.new_b2b_corporate_flag = fact.new_b2b_corporate_flag 	)
  and (tr.is_supplier_flight_not_found_flag = fact.is_supplier_flight_not_found_flag 	)
  and (tr.add_ons_hotel_detail_json = fact.add_ons_hotel_detail_json 	or (tr.add_ons_hotel_detail_json is null and fact.add_ons_hotel_detail_json is null))
  and (tr.halodoc_sell_price_amount = fact.halodoc_sell_price_amount)
  and (tr.halodoc_pax_count = fact.halodoc_pax_count or (tr.halodoc_pax_count is null and fact.halodoc_pax_count is null))
  and (tr.memo_halodoc = fact.memo_halodoc or (tr.memo_halodoc is null and fact.memo_halodoc is null))
  and (tr.is_has_halodoc_flag = fact.is_has_halodoc_flag or (tr.is_has_halodoc_flag is null and fact.is_has_halodoc_flag is null))
  and coalesce(tr.convenience_fee_amount,0) = coalesce(tr.convenience_fee_amount,0)
  and (tr.memo_convenience_fee = fact.memo_convenience_fee or (tr.memo_convenience_fee is null and fact.memo_convenience_fee is null))
  and (tr.giftcard_voucher_user_email_reference_id = fact.giftcard_voucher_user_email_reference_id or (tr.giftcard_voucher_user_email_reference_id is null and fact.giftcard_voucher_user_email_reference_id is null))
  and (tr.giftcard_voucher_purpose = fact.giftcard_voucher_purpose or (tr.giftcard_voucher_purpose is null and fact.giftcard_voucher_purpose is null))
  and (tr.memo_giftvoucher = fact.memo_giftvoucher or (tr.memo_giftvoucher is null and fact.memo_giftvoucher is null))
  and (tr.old_id_rebooking = fact.old_id_rebooking or (tr.old_id_rebooking is null and fact.old_id_rebooking is null))
  and (tr.diff_amount_rebooking = fact.diff_amount_rebooking or (tr.diff_amount_rebooking is null and fact.diff_amount_rebooking is null))
  and (tr.is_rebooking_flag = fact.is_rebooking_flag or (tr.is_rebooking_flag is null and fact.is_rebooking_flag is null))
  and (tr.is_flexi_reschedule = fact.is_flexi_reschedule or (tr.is_flexi_reschedule is null and fact.is_flexi_reschedule is null))
  and (tr.flexi_fare_diff = fact.flexi_fare_diff or (tr.flexi_fare_diff is null and fact.flexi_fare_diff is null))
  and (tr.flexi_reschedule_fee = fact.flexi_reschedule_fee or (tr.flexi_reschedule_fee is null and fact.flexi_reschedule_fee is null))
  and (tr.partner_commission = fact.partner_commission or (tr.partner_commission is null and fact.partner_commission is null))
  and (tr.subsidy_category = fact.subsidy_category or (tr.subsidy_category is null and fact.subsidy_category is null))
where
  tr.order_id is null
)
select
  *
from append