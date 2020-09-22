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
    `prod-datarangers.galaxy_stg.workday_mapping_event_supplier` 
)
, master_event_product_provider as (
  select
    *
  from
    `prod-datarangers.galaxy_stg.workday_mapping_event_product_provider`  
)
, master_event_name_deposit as (
  select
    event_name
    , 'Deposit' as deposit_flag
    , start_date
    , coalesce(end_date, '2100-12-31') as end_date
  from
    `prod-datarangers.galaxy_stg.workday_mapping_deposit_event` 
  where is_deposit_flag = true
)
, master_event_supplier_deposit as (
  select
    supplier_id
    , workday_supplier_reference_id
    , is_deposit_flag as is_event_supplier_deposit_flag 
  from
    `prod-datarangers.galaxy_stg.workday_mapping_supplier` 
  where lower(supplier_category) = 'event'
)
, master_supplier_airport_transfer_and_lounge as (
  select 
    workday_supplier_reference_id
    , workday_supplier_name
    , is_deposit_flag
  from 
    `prod-datarangers.galaxy_stg.workday_mapping_supplier`
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
    `prod-datarangers.galaxy_stg.corporate_account`
  where
    workday_business_id is not null
)
, ma_ori as (
  select
    distinct
    account_id
    , lower(replace(replace(account_username,'"',''),'\\','')) as account_username
    , account_last_login as accountlastlogin
    , processed_dttm
  from
    `prod-datarangers.galaxy_stg.member__account`
  union distinct
  select
    distinct
    accountid as account_id
    , lower(replace(replace(accountusername,'"',''),'\\','')) as account_username
    , accountlastlogin
    , processed_dttm
  from
    `prod-datarangers.galaxy_stg.members_account_admin` 
  union distinct
  select
    distinct
    accountid as account_id
    , lower(replace(replace(accountusername,'"',''),'\\','')) as account_username
    , accountlastlogin
    , processed_dttm
  from
    `prod-datarangers.galaxy_stg.members_account_b2c` 
  union distinct
  select
    distinct
    accountid as account_id
    , lower(replace(replace(accountusername,'"',''),'\\','')) as account_username
    , accountlastlogin
    , processed_dttm
  from
    `prod-datarangers.galaxy_stg.members_account_b2b` 
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
    , corporate_flag
    , business_id
    , reseller_id
  from
    `prod-datarangers.galaxy_stg.order__cart`
    left join ma using (account_id)
    left join ca using (account_username)
  where
    payment_timestamp >= (select filter1 from fd)
    and payment_timestamp <= (select filter3 from fd)
    and payment_status = 'paid'
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
        when  order_type  = "flight"
          then order_name
        when  order_type  = "car"
          then order_name
        when  order_type  = "train"
          then 'PT. Kereta Api Indonesia'
        when  order_type  in ("insurance","cancel_insurance")
          then order_name_detail
        else order_name
      end as order_detail_name
    , replace(order_name,'"','') as order_name
    , order_name_detail
    , order_master_id as product_provider
  from
    `prod-datarangers.galaxy_stg.order__cart_detail`
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp <= (select filter3 from fd)
    and order_type in ('flight','car','hotel','tixhotel','event','train','insurance','cancel_insurance','tix','airport_transfer')
)
, oci as (
  select
    order_detail_id
    , string_agg(distinct issue_code) as insurance_issue_code
  from
    `prod-datarangers.galaxy_stg.order__cart_insurance` 
  group by
    1
)
, occi as (
  select
    order_detail_id
    , string_agg(distinct issue_code) as cancel_insurance_issue_code
  from
    `prod-datarangers.galaxy_stg.order__cart_cancel_insurance` 
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
    `prod-datarangers.galaxy_stg.order__payment`
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
        `prod-datarangers.galaxy_stg.order__cart_cancel_insurance_pax` 
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
    `prod-datarangers.galaxy_stg.workday_mapping_supplier`
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
    `prod-datarangers.galaxy_stg.workday_mapping_supplier`
  where
    vendor = 'sa'
)
, ocfp as (
  select
    order_detail_id
    , string_agg(ticket_number order by order_passenger_id desc) as ticket_number
  from
    `prod-datarangers.galaxy_stg.order__cart_flight_passenger`
  group by
    order_detail_id
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
        when ocf.vendor = 'sa' then wsr_name.workday_supplier_reference_id
        when ocf.vendor = 'na' then wsr_id.workday_supplier_reference_id
      end as airlines_master_id
    , safe_cast(ocf.balance_due as float64) as balance_due
    , safe_cast(ocf.price_nta as float64) as price_nta
    , safe_cast(ocf.count_adult+ocf.count_child+ocf.count_infant as int64) as total_pax
    , datetime(ocf.departure_time, 'Asia/Jakarta') as departure_time
    , safe_cast(ocf.baggage_fee as float64) as baggage_fee
    , case
        when ocf.vendor = 'sa' and wsr_name.is_deposit_flag is null then 'Non deposit'
        else 'Deposit'
      end as deposit_flag_flight
    , ticket_number
  from
    `prod-datarangers.galaxy_stg.order__cart_flight` ocf
    left join wsr_id using (airlines_master_id,vendor)
    left join wsr_name on ocf.account = wsr_name.supplier_name and ocf.vendor = 'sa'
    left join ocfp on ocf.order_detail_id = ocfp.order_detail_id
  where
    departure_time >= (select filter2 from fd)
)
, occar as (
  select
    occar.order_detail_id
    , (timestamp_diff(max(occar.checkin_date),min(occar.checkin_date),day)+1) * max(occar.qty) as quantity_car
    , safe_cast(sum(occar.net_rate_price) as float64) as net_rate_price_car
    , string_agg(distinct occar.net_rate_currency) as net_rate_currency_car
    , string_agg(distinct split(split(log_data,'business_id":')[safe_offset(1)],',"')[safe_offset(0)])  as supplier_id_car
    , max(safe_divide(customer_price,sell_rate_price)) as kurs_car
    , datetime(min(occar.checkin_date), 'Asia/Jakarta') as min_checkin_date_car
  from 
    `prod-datarangers.galaxy_stg.order__cart_car` occar
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
    `prod-datarangers.galaxy_stg.order__cart_train`
  where
    departure_datetime >= (select filter2 from fd)
  group by order_detail_id
)
, oce as (
  select
    order_detail_id
    , max(order_tiket_number) as quantity_event
    , sum(customer_price) as customer_price
    , string_agg(distinct net_rate_currency) as net_rate_currency_event
    , max(safe_divide(customer_price,sell_rate_price)) as kurs_event
    , string_agg(tiket_barcode order by order_detail_id desc, order_tiket_number desc) as tiket_barcode_event
  from
    `prod-datarangers.galaxy_stg.order__cart_event`
  where 
    checkin_date >= (select filter2 from fd)
    or checkin_date is null
  group by
    order_detail_id
)
, decm as (
  select
    distinct 
    detail_id as detail_event_id
    , string_agg(distinct case
        when length(business_id) = 0 then '(blank)'
        when business_id is null then '(null)'
        else business_id
      end) as product_provider_event
    , string_agg(distinct event_type) as event_type
    , string_agg(distinct replace(event_name,'"','')) as event_name
    , string_agg(event_category) as event_category
    , string_agg(distinct ext_source) as ext_source_event
    , max(tiket_comission) as tiket_comission
    , max(sellprice) as sellprice
    , max(sellprice_netto) as sellprice_netto
    , datetime(min(case 
          when event_type = 'D' or lower(event_name) like '%paddles%' then profile_event_start
          else tiket_event_start
        end), 'Asia/Jakarta') as event_datetime
    , string_agg(distinct case
        when lower(event_name) like ('%railink%') then 'VR-00000026'
        when length(decm.supplier_id) = 0 then '(blank)'
        when decm.supplier_id is null then '(null)'
        else coalesce(msatl.workday_supplier_reference_id,decm.supplier_id)
      end) as supplier_id_event
    , max(sellprice_adult) as sellprice_adult
    , max(sellprice_child) as sellprice_child
    , max(sellprice_infant) as sellprice_infant
    , max(sellprice_senior) as sellprice_senior
    , max(fee_in_price) as fee_in_price
    , max(tax_percent_in_price) as tax_percent_in_price
    , max(coalesce(is_event_supplier_deposit_flag, is_deposit_flag)) as is_deposit_flag
  from
    `prod-datarangers.galaxy_stg.detail__event_connect_ms` decm
    left join master_supplier_airport_transfer_and_lounge msatl 
      on msatl.workday_supplier_name = case
                                        when decm.event_name like ('Airport Transfer%') then 'Airport Transfer'
                                        when decm.event_name like ('Tix-Spot Airport Lounge%') then 'Tix-Sport Airport Lounge' 
                                      end
    left join master_event_supplier_deposit mesd
      on safe_cast(mesd.supplier_id as string) = decm.supplier_id
  group by
    detail_id
)
, oecm as (
  select
    distinct
    order_detail_id
    , order_id
    , detail_event_id
    , qty_adult
    , qty_child
    , qty_infant
    , qty_senior
    , is_tiketflexi
  from
    `prod-datarangers.galaxy_stg.order__event_connect_ms`
    
)
, event_order as ( /* use this because commission in oecm is not rounded, but floor-ed, example order id 104001549 */
  select
    order_id
    , round(sum(commission)) as commission
    , round(sum(base_price)) as base_price
  from
    (
      select 
        _id
        , safe_cast(coreOrderId as int64) order_id
        , pt.commissionInCents.numberLong/100 commission
        , pt.basePriceInCents.numberLong/100 base_price
        , rank() over(partition by coreorderId, pt.code order by lastModifiedDate desc) rownum
      from 
        `prod-datarangers.galaxy_stg_intermediary.events_v2_order__order_l2` o
        left join unnest (priceTierQuantities) pt
        left join unnest(tickets) tic on /*to get the same pricetierquantities code as the tickets*/
      lower(tic.priceTierCode) = lower(pt.code)
    )
  where
    rownum = 1
  group by 
    1
)
, oce_fact as (
  select
    * except(order_id)
    , case 
        when event_category = 'HOTEL' then 'Hotel'
        when event_name like ('Airport Transfer%') then 'Car'
        when lower(event_name) LIKE'sewa mobil%' AND event_category='TRANSPORT' then 'Car'  --Update by Rizki Habibie @2020, 18th of August
        when event_name like ('Tix-Spot Airport Lounge%') then 'Others'
        when lower(event_name) like ('%railink%') then 'Train'
        when event_type = 'D' then 'Attraction'
        when event_type = 'E' then 'Activity'        
        else 'Event'
      end as event_type_name
    , case
      when is_deposit_flag = true then 'Deposit'
      when ext_source_event = 'BE_MY_GUEST' then 'Deposit'
      else 'Non deposit'
    end as deposit_flag_event
    , case 
        when event_category = 'HOTEL' then coalesce(eo.commission,coalesce(sellprice,0) - coalesce(sellprice_netto,0))
        when tiket_comission > 100 then 0
        else safe_divide((((qty_adult * sellprice_adult) + (qty_child * sellprice_child) + (qty_infant * sellprice_infant) + (qty_senior * sellprice_senior)) - fee_in_price) * tiket_comission, (100+tax_percent_in_price)) 
      end as commission_event
  from
    oce
    left join oecm using (order_detail_id)
    left join decm using (detail_event_id)
    left join event_order eo using (order_id)
)
, master_category_add_ons as (
  select
    add_ons_name as category_code
    , spend_category_name
  from 
    `prod-datarangers.galaxy_stg.workday_mapping_category_hotel_add_on`
)
, oth as (
  select
    distinct
    order_id
    , hotel_itinerarynumber
    , hotel_id as hotel_id_oth
    , datetime(booking_checkindate, 'Asia/Jakarta') as booking_checkindate
    , safe_cast(nett_price as float64) as nett_price
    , round(rebooking_price) as rebooking_price_hotel
    , room_source
    , json_extract_scalar(additional_info,'$.name') as room_source_info
    , booking_room * booking_night as room_night
    , coalesce(markup_percentage,0) as markup_percentage_hotel
    , vendor_incentive
    , auto_subsidy_value
    , order_issued
  from
    `prod-datarangers.galaxy_stg.order__tixhotel`
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp <= (select filter3 from fd)
    and hotel_itinerarynumber is not null 
)
, hb as (
  select
    distinct
    safe_cast(id as string) as hotel_itinerarynumber
    , currency_exchange_rate as kurs
    , hotel_id
  from
    `prod-datarangers.galaxy_stg.hotel_bookings`
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
    from `prod-datarangers.galaxy_stg.hotel_booking_details`) a
  group by itinerary_id
)
, hbao as (
  select 
    safe_cast(hbao.itinerary_id as string) as hotel_itinerarynumber
    , category_code
    , sum(amount) as add_ons_hotel_quantity
    , sum(round(total_net_rate_price,2)) as add_ons_hotel_net_price_amount
  from 
    `prod-datarangers.galaxy_stg.hotel_booking_add_ons` hbao
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
    `prod-datarangers.galaxy_stg.hotel__payment_type` 
  where
    status = 'active'
)
, oth_fact as (
  select
    order_id
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
      end as product_provider_hotel
    , hotel_id_oth
    , case
        when room_source = 'TIKET' then net_rate_currency
        else 'IDR'
      end as net_rate_currency_hotel
    , booking_checkindate
    , case
        when room_source = 'TIKET' then total_net_rate_price
        when room_source like '%AGODA%' then case when rebooking_price_hotel > 0 then rebooking_price_hotel - vendor_incentive else nett_price - vendor_incentive end
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
)
, bp as (
  select
    business_id as product_provider_bp
    , business_name as business_name
  from
    `prod-datarangers.galaxy_stg.business__profile` 
)
, ac as (
  select
    distinct
    master_id as product_provider_ac
    , string_agg(distinct airlines_name) as airlines_name
  from
    `prod-datarangers.galaxy_stg.airlines_code`
  group by
    master_id
)
, cv as (
  select
    master_id
    , string_agg(distinct vendor_name) as vendor_name_car
  from
    `prod-datarangers.galaxy_stg.car__vendor`
  group by
    master_id
)
/* get data cogs for order tix*/
, octd as (
  select
    order_detail_id
    , sum(product_qty * product_seller_idr) as net_rate_price_tix
  from 
    `prod-datarangers.galaxy_stg.order__cart_tix_detail`
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
        `prod-datarangers.galaxy_stg_intermediary.apt_order__apt_order`
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
        when ocd.order_type in ('train') then concat(safe_cast(oc.order_id as string), ' - ', ocd.order_name_detail, ' / ', ocd.order_name)
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
    left join oce_fact using (order_detail_id)
    left join oci using (order_detail_id)
    left join occi using (order_detail_id)
    left join apt_fact using (order_detail_id)
    left join bp on order_type not in ('flight','tixhotel') and ocd.product_provider = bp.product_provider_bp
    left join ac on order_type in ('flight') and ocd.product_provider = ac.product_provider_ac
    left join octd using (order_detail_id)
  where
    (order_type = 'flight' and ocf.ticket_status = 'issued')
    or
    (order_type = 'tixhotel' and oth_fact.order_issued = 1)
    or
    (order_type in ('insurance','cancel_insurance','car','train','event','tix','airport_transfer'))
)
, fact_product as (
  select
    'GTN_IDN' as Company
    , case
        when string_agg(distinct order_type) in ('flight','train','tix','airport_transfer') then 'IDR'
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
        when string_agg(distinct order_type) = 'tixhotel' then max(booking_checkindate)
        when string_agg(distinct order_type) = 'car' then max(min_checkin_date_car)
        when string_agg(distinct order_type) = 'event' and string_agg(distinct lower(order_detail_name)) like 'sewa mobil%' then date_add(date(max(payment_timestamp_oc)), interval 1 day)
        when string_agg(distinct order_type) = 'train' then 
          case when date(max(payment_timestamp_oc)) >= '2020-04-01' then max(payment_timestamp_oc)
          else max(arrival_datetime_train)
          end
        when string_agg(distinct order_type) = 'event' then max(event_datetime)
        when string_agg(distinct order_type) = 'airport_transfer' then max(airport_transfer_pickup_datetime)
        else null
      end as schedule_date
    , string_agg(distinct replace(order_detail_name,'"','')) as order_detail_name
    , case
        when string_agg(distinct order_type) = 'flight' then 'Ticket'
        when string_agg(distinct order_type) = 'tixhotel' then 'Room'
        when string_agg(distinct order_type) = 'car' then 'Rental'
        when string_agg(distinct order_type) = 'train' then 'Ticket'
        when string_agg(distinct order_type) = 'event' then
          case
            when string_agg(distinct event_type_name) = 'Hotel' then 'Hotel_Voucher'
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
        when string_agg(distinct order_type) = 'event' then max(quantity_event)
        when string_agg(distinct order_type) = 'tix' then 1
        when string_agg(distinct order_type) = 'airport_transfer' then max(quantity_airport_transfer)
        else null
      end as quantity
    , round(case
        when string_agg(distinct order_type) = 'flight' 
          then sum(round(
            case 
              when airlines_master_id in ('VR-00000006','VR-00000011','VR-00000004','VR-00000007') and date(payment_timestamp_oc) >= '2020-05-11' then price_nta /* 13 May 2020, Anggi Anggara: for lion group, start order >= 2020-05-11 using price_nta*//* 27 May 2020, Anggi Anggara: for trigana, sriwjaya , transnusa, start order >= 2020-05-11 using price_nta*/
              else balance_due
            end - baggage_fee))
        when string_agg(distinct order_type) = 'tixhotel' then sum(total_net_rate_price)
        when string_agg(distinct order_type) = 'car' then sum(selling_price)
        when string_agg(distinct order_type) = 'train' then sum(net_rate_price_train)
        when string_agg(distinct order_type) = 'event' then max(selling_price - round(coalesce(commission_event,0)))
        when string_agg(distinct order_type) = 'tix' then max(net_rate_price_tix)
        when string_agg(distinct order_type) = 'airport_transfer' then max(cogs_airport_transfer)
        else null
      end,2) as total_line_amount
    , round(case
        when string_agg(distinct order_type) = 'flight' then 1
        when string_agg(distinct order_type) = 'tixhotel' then max(kurs_hotel)
        when string_agg(distinct order_type) = 'car' then max(kurs_car)
        when string_agg(distinct order_type) = 'train' then 1
        when string_agg(distinct order_type) = 'event' then max(kurs_event)
        when string_agg(distinct order_type) = 'tix' then 1
        when string_agg(distinct order_type) = 'airport_transfer' then 1
        else null
      end,2) as currency_conversion
    , case
        when string_agg(distinct order_type) = 'flight' then string_agg(booking_code order by order_detail_id)
        when string_agg(distinct order_type) = 'tixhotel' then string_agg(hotel_itinerarynumber order by order_detail_id)
        when string_agg(distinct order_type) = 'train' then string_agg(booking_code_train order by order_detail_id)
        when string_agg(distinct order_type) = 'event' then string_agg(tiket_barcode_event order by order_detail_id)
        else null
      end as booking_code
    , case
        when string_agg(distinct order_type) = 'flight' then 'Flight'
        when string_agg(distinct order_type) = 'tixhotel' then 'Hotel'
        when string_agg(distinct order_type) = 'car' then 'Car'
        when string_agg(distinct order_type) = 'train' then 'Train'
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
        when string_agg(distinct order_type) = 'tix' then 'Tiketpoint_redemeed'
        when string_agg(distinct order_type) = 'airport_transfer' then string_agg(distinct new_product_provider_id)
        else string_agg(distinct safe_cast(product_provider as string))
      end as product_provider
    , case
        when string_agg(distinct order_type) = 'flight' then string_agg(distinct deposit_flag_flight)
        when string_agg(distinct order_type) = 'tixhotel' then string_agg(distinct deposit_flag_hotel)
        when string_agg(distinct order_type) = 'car' then 
          case 
            when string_agg(distinct order_detail_name) like '%EXTRA%' or string_agg(distinct order_detail_name) like '%BEST PRICE%' then 'Deposit'
            else 'Non deposit'
          end
        when string_agg(distinct order_type) = 'train' then 'Deposit'
        when string_agg(distinct order_type) = 'event' then string_agg(distinct coalesce(mend.deposit_flag,deposit_flag_event))
        when string_agg(distinct order_type) = 'tix' then 'Non deposit'
        when string_agg(distinct order_type) = 'airport_transfer' then 'Deposit'
        else null
      end as deposit_flag
    , case
        when string_agg(distinct order_type) = 'event' then string_agg(distinct replace(order_detail_name,'"',''))
        else null
      end as event_name
    , string_agg(distinct memo) as memo
    , sum(round(baggage_fee)) as baggage_fee
    , array_concat_agg(add_ons_hotel_detail_array) as add_ons_hotel_detail_array
    /* 25 May 2020: add customer reference id for SI, for B2C use value 'C-000001'*/
    , case
          when date(max(payment_timestamp_oc)) >= '2020-04-06' and string_agg(distinct corporate_flag) is not null and string_agg(distinct payment_source)  in ('cash_onsite') then string_agg(distinct business_id) /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when date(max(payment_timestamp_oc)) <'2020-04-06' and string_agg(distinct corporate_flag) is not null then string_agg(distinct business_id) /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when 
            date(max(payment_timestamp_oc)) >= '2020-04-06' and string_agg(distinct reseller_type) in ('none','online_marketing','native_apps')
            and (string_agg(distinct corporate_flag) is null or (string_agg(distinct corporate_flag) is not null and string_agg(distinct payment_source) not in ('cash_onsite')))
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
    order_type in ('flight','tixhotel','car','train','event', 'tix','airport_transfer')
  group by 
    order_id
    , order_detail_id
)
, fact_vertical as (
  select
    Company
    , invoice_currency
    , supplier_reference_id
    , invoice_date
    , order_id
    , order_detail_id
    , case 
        when spend_category = 'Hotel_Voucher' then datetime_add(invoice_date, interval 2 day)
        when invoice_date > schedule_date then invoice_date
        else schedule_date
      end as due_date
    , order_detail_name
    , spend_category
    , coalesce(quantity,1) as quantity
    , total_line_amount
    , currency_conversion
    , booking_code
    , product_category
    , product_provider
    , deposit_flag
    , event_name
    , null as payment_handling
    , case 
        when deposit_flag = 'Non deposit' then 'Yes'
        else 'No'
      end as on_hold_status
    , memo
    , customer_reference_id
  from
    fact_product
)
, fact_baggage as (
  select
    Company
    , invoice_currency
    , supplier_reference_id
    , invoice_date
    , order_id
    , order_detail_id
    , case 
        when spend_category = 'Hotel_Voucher' then datetime_add(invoice_date, interval 2 day)
        when invoice_date > schedule_date then invoice_date
        else schedule_date
      end as due_date
    , order_detail_name
    , 'Bagage' as spend_category
    , 1 as quantity
    , baggage_fee as total_line_amount
    , currency_conversion
    , booking_code
    , product_category
    , product_provider
    , deposit_flag
    , event_name
    , null as payment_handling
    , case 
        when deposit_flag = 'Non deposit' then 'Yes'
        else 'No'
      end as on_hold_status
    , memo
    , customer_reference_id
  from
    fact_product
  where baggage_fee > 0
)
, fact_add_ons_hotel as (
  select
    Company
    , invoice_currency
    , supplier_reference_id
    , invoice_date
    , order_id
    , order_detail_id
    , case 
        when spend_category = 'Hotel_Voucher' then datetime_add(invoice_date, interval 2 day)
        when invoice_date > schedule_date then invoice_date
        else schedule_date
      end as due_date
    , order_detail_name
    , add_ons_hotel.add_ons_spend_category
    , coalesce(add_ons_hotel.add_ons_hotel_quantity,1) as quantity
    , add_ons_hotel.add_ons_hotel_net_price_amount as total_line_amount
    , currency_conversion
    , booking_code
    , product_category
    , product_provider
    , deposit_flag
    , event_name
    , null as payment_handling
    , case 
        when deposit_flag = 'Non deposit' then 'Yes'
        else 'No'
      end as on_hold_status
    , memo
    , customer_reference_id
  from
    fact_product
    cross join 
       unnest(add_ons_hotel_detail_array) as add_ons_hotel
)
, fact as (
select * from fact_vertical
union all
select * from fact_baggage
union all
select * from fact_add_ons_hotel
)
, tr as (
  select 
    * except (rn)
  from
    (
      select
        *
        , row_number() over(partition by order_id, order_detail_id, spend_category order by processed_timestamp desc) as rn
      from
        `datamart-finance.datasource_workday.supplier_invoice_raw`
      where date(invoice_date) >= (select date(filter1,'Asia/Jakarta') from fd)
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
    (tr.Company = fact.Company)
  and (tr.invoice_currency = fact.invoice_currency or (tr.invoice_currency is null and fact.invoice_currency is null))
  and (tr.supplier_reference_id = fact.supplier_reference_id or (tr.supplier_reference_id is null and fact.supplier_reference_id is null))
  and (tr.invoice_date = fact.invoice_date)
  and (tr.order_id = fact.order_id)
  and (tr.order_detail_id = fact.order_detail_id)
  and (tr.due_date = fact.due_date)
  and (tr.order_detail_name = fact.order_detail_name)
  and (tr.spend_category = fact.spend_category)
  and (tr.quantity = fact.quantity or (tr.quantity is null and fact.quantity is null))
  and (tr.total_line_amount = fact.total_line_amount or (tr.total_line_amount is null and fact.total_line_amount is null))
  and (tr.currency_conversion = fact.currency_conversion or (tr.currency_conversion is null and fact.currency_conversion is null))
  and (tr.booking_code = fact.booking_code or (tr.booking_code is null and fact.booking_code is null))
  and (tr.product_category = fact.product_category)
  and (tr.product_provider = fact.product_provider or (tr.product_provider is null and fact.product_provider is null))
  and (tr.deposit_flag = fact.deposit_flag or (tr.deposit_flag is null and fact.deposit_flag is null))
  and (tr.event_name = fact.event_name or (tr.event_name is null and fact.event_name is null))
  and (tr.payment_handling = fact.payment_handling or (tr.payment_handling is null and fact.payment_handling is null))
  and (tr.on_hold_status = fact.on_hold_status or (tr.on_hold_status is null and fact.on_hold_status is null))
  and (tr.memo = fact.memo or (tr.memo is null and fact.memo is null))
  and (tr.customer_reference_id = fact.customer_reference_id or (tr.customer_reference_id is null and fact.customer_reference_id is null))
where
  tr.order_detail_id is null
)
select * from append