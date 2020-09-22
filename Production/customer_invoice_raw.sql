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
, 
wsr_id as (
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
, wmpc as (
  select
    *
  from
    `datamart-finance.staging.v_workday_mapping_payment_charge` 
)
, fpm as (
  select 
    payment_source
    , payment_type_workday as payment_type_bank
  from
    `datamart-finance.staging.v_workday_mapping_payment_bank`
  where
    pg_name is null
  group by /*need to be grouped by because there are payment_source that has 2 types of bank_id*/
    1,2
)
, fpm2 as (
  select 
    payment_source
    , pg_name as payment_gateway
    , acquiring_bank
    , payment_type_workday as payment_type_bank
  from 
    `datamart-finance.staging.v_workday_mapping_payment_bank`
  where
    pg_name is not null
  group by
    1,2,3,4
)
, gcc as (
  select
    gift_order_detail_id as order_detail_id_gf
    , string_agg(distinct replace(gift_description,'\n',' ')) as giftcard_voucher
  from 
    `datamart-finance.staging.v_giftcard__codes` 
  where
    gift_order_detail_id is not null
  group by
    gift_order_detail_id 
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
, master_supplier as (
  select
    * except(processed_date)
  , case 
      when lead(date_add(processed_date, interval -1 day)) over(partition by supplier_id order by processed_date desc) is null
        then date('2000-01-01')
      else date_add(processed_date, interval -1 day)
    end as start_date
  , date_add(processed_date, interval -1 day) as active_date
  , coalesce(lag(date_add(processed_date, interval -2 day)) over(partition by supplier_id order by processed_date desc), date('2099-12-31')) as end_date
from
    `datamart-finance.datasource_workday.master_supplier`
)
, master_product_provider as (
  select
    * except(processed_date)
  , case 
      when lead(date_add(processed_date, interval -1 day)) over(partition by product_provider_id order by processed_date desc) is null
        then date('2000-01-01')
      else date_add(processed_date, interval -1 day)
    end as start_date
  , date_add(processed_date, interval -1 day) as active_date
  , coalesce(lag(date_add(processed_date, interval -2 day)) over(partition by product_provider_id order by processed_date desc), date('2099-12-31')) as end_date
from
    `datamart-finance.datasource_workday.master_product_provider`
)
, master_b2b_corporate as (
  select
    * except(processed_date)
  , case 
      when lead(date_add(processed_date, interval -1 day)) over(partition by business_id order by processed_date desc) is null
        then date('2000-01-01')
      else date_add(processed_date, interval -1 day)
    end as start_date
  , date_add(processed_date, interval -1 day) as active_date
  , coalesce(lag(date_add(processed_date, interval -2 day)) over(partition by business_id order by processed_date desc), date('2099-12-31')) as end_date
from
    `datamart-finance.datasource_workday.master_b2b_corporate`
)
, master_b2b_online_and_offline as (
  select
    * except(processed_date)
  , case 
      when lead(date_add(processed_date, interval -1 day)) over(partition by business_id order by processed_date desc) is null
        then date('2000-01-01')
      else date_add(processed_date, interval -1 day)
    end as start_date
  , date_add(processed_date, interval -1 day) as active_date
  , coalesce(lag(date_add(processed_date, interval -2 day)) over(partition by business_id order by processed_date desc), date('2099-12-31')) as end_date
from
    `datamart-finance.datasource_workday.master_b2b_online_and_offline`
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
, master_supplier_airport_transfer_and_lounge as (
  select 
    workday_supplier_reference_id
    , workday_supplier_name
  from 
    `datamart-finance.staging.v_workday_mapping_supplier`
  where 
    workday_supplier_name in ('Airport Transfer','Tix-Sport Airport Lounge')
  group by 
    1,2
)
, oc as (
  select
    distinct
    order_id
    , datetime(payment_timestamp, 'Asia/Jakarta') as payment_timestamp
    , reseller_type
    , account_id
    , reseller_id
    , cc_installment 
    , total_customer_price
  from  
    `datamart-finance.staging.v_order__cart`
  where
    payment_status = 'paid'
    and payment_timestamp >= (select filter1 from fd)
    and payment_timestamp < (select filter3 from fd)
)
, op as (
  select
    distinct
    order_id
    , payment_source
    , payment_amount
  from
    `datamart-finance.staging.v_order__payment`
  where
    payment_id = 1
    and payment_flag = 1
    and payment_timestamp >= (select filter2 from fd)
    and payment_timestamp < (select filter3 from fd)
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
, ocd as (
  select
    distinct
    order_id
    , order_type
    , order_detail_id
    , customer_price
    , selling_currency
    , selling_price
    , order_master_id
    , order_name
    , order_name_detail
    , safe_divide(selling_price,sum(selling_price) over(partition by order_id)) as selling_price_proportion_value
    , max(order_detail_id) over(partition by order_id) as last_order_detail_id
  from
    ocd_or
  where
    order_type in ('flight','car','hotel','tixhotel','train','event', 'airport_transfer')
)
, ocdv as (
  select
    distinct
    order_id
    , order_type
  from
    ocd_or
  where
    order_type in ('flight','car','hotel','tixhotel','train','event', 'airport_transfer')
)
, ocdp as (
  select
    order_id 
    , string_agg(order_name order by order_detail_id asc) as payment_order_name
    , safe_cast(sum(customer_price) as float64) as payment_charge
  from
    ocd_or
  where
    order_type in ('payment')
  group by 1
)
, ocdgv as (
  select
    order_id 
    , string_agg(distinct order_name) as giftvoucher_name
    , safe_cast(sum(customer_price) as float64) as giftvoucher_value
    , string_agg(distinct coalesce(gcc.giftcard_voucher,replace(tix_gcc.giftcard_voucher,'"',''))) as giftcard_voucher
    , string_agg(distinct giftcard_voucher_purpose) as giftcard_voucher_purpose
    , string_agg(distinct giftcard_voucher_user_email_reference_id) as giftcard_voucher_user_email_reference_id
  from
    ocd_or
    left join gcc on ocd_or.order_detail_id = gcc.order_detail_id_gf
    left join tix_gcc using (order_id)
  where
    order_type in ('giftcard')
  group by 1
)
, ocdpc as (
  select
    order_id 
    , string_agg(distinct order_name) as promocode_name
    , safe_cast(sum(customer_price) as float64) as promocode_value
  from
    ocd_or
  where
    order_type in ('promocode')
  group by 1
)
, ocdtp as (
  select 
    order_id 
    , safe_cast(sum(customer_price) as float64) as tiketpoint_value
  from
    ocd_or
  where
    order_type in ('tiketpoint')
  group by 1
)
, ocdrd as (
  select
    order_id 
    , string_agg(distinct order_name) as refund_deposit_name
    , safe_cast(sum(customer_price) as float64) as refund_deposit_value
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
    order_detail_id as cancel_insurance_order_detail_id
    , string_agg(distinct issue_code) as cancel_insurance_issue_code
  from
    `datamart-finance.staging.v_order__cart_cancel_insurance` 
  group by
    1
)
, ocdi as ( /* 6 Sept 2020: Change query for multi insurance. All the insurances will be in the array, hence the table's behavior will be change since the deployment of this query.*/
  select
    order_id 
    , safe_cast(split(split(order_name_detail,'#') [safe_offset(1)],' ') [safe_offset(0)] as int64) as order_detail_id
    , safe_cast(sum(customer_price) as float64) as insurance_value
    , string_agg(order_name order by order_detail_id) as insurance_name
    , string_agg(insurance_issue_code order by order_detail_id) as insurance_issue_code
    , concat('[',string_agg(concat('{"order_detail_id":"',order_detail_id,'",'
      ,'"insurance_value":"',coalesce(safe_cast(customer_price as string),'No Insurance Value Set'),'",'
      ,'"insurance_name":"',coalesce(order_name, 'No Insurance Name Set'),'",'
      ,'"insurance_issue_code":"',coalesce(safe_cast(insurance_issue_code as string),'Insurance not issued'),'",'
      ,'"memo_insurance":"',concat(safe_cast(order_id as string), ' - ', 'Cermati BCA Insurance, Issue Code: ', ifnull(insurance_issue_code,''))
      ,'"}') order by order_detail_id asc),']') as flight_insurance_json
  from
    ocd_or
    left join oci using (order_detail_id)
  where
    order_type in ('insurance')
  group by 1,2
)
, ocdci as (
  select
    order_id 
    , safe_cast(split(split(order_name_detail,'#') [safe_offset(1)],' ') [safe_offset(0)] as int64) as order_detail_id
    , order_detail_id as cancel_insurance_order_detail_id
    , string_agg(distinct order_name_detail) as cancel_insurance_name
    , safe_cast(sum(customer_price) as float64) as cancel_insurance_value
  from
    ocd_or
  where
    order_type in ('cancel_insurance')
  group by 1,2,3
)
, ocdcf as (
  select
    order_id
    , string_agg(distinct order_name) as convenience_fee_order_name
    , safe_cast(max(selling_price) as float64) as convenience_fee_amount
  from
    ocd_or
  where
    order_type = 'convenience_fee'
  group by
    1
)
, occ as (
  select
    distinct
    order_id
    , string_agg(distinct auth_code ) as auth_code
    , string_agg(distinct pg_name) as payment_gateway
    , string_agg(distinct acquiring_bank) as acquiring_bank
  from
    `datamart-finance.staging.v_order__credit_card`
  where
    payment_timestamp >= (select filter2 from fd)
    and payment_timestamp < (select filter3 from fd)
  group by
    order_id
)
, onp as (
  select
    distinct
    order_id
    , concat("'",safe_cast(virtual_account as string)) as virtual_account
  from
    (
      select
        order_id
        , virtual_account
        , row_number() over(partition by order_id order by id desc) as rn
      from
        `datamart-finance.staging.v_order__nicepay`
      where
        created_timestamp >= (select filter2 from fd)
    )
  where
    rn = 1
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
, fact_car as (
  select
    order_detail_id
    , vendor as product_provider_car
    , vendor as supplier_car
    , 0 as commission_car
    , 0 as upselling_car
    , 0 as subsidy_car
    , quantity_car
    , 'Rental' as revenue_category_car
    , 'Car' as product_category_car
  from
    occar
)

/* Fact event */
, oce as (
  select
    order_detail_id
    , max(order_tiket_number) as quantity_event
    , string_agg(tiket_barcode order by order_detail_id desc, order_tiket_number desc) as ticket_number_event
  from
    `datamart-finance.staging.v_order__cart_event` 
  group by
    order_detail_id
)
, decm as (
  select
    distinct
    detail_id as detail_event_id
    , business_id
    , event_name
    , event_type
    , ext_source
    , event_category
    , supplier_id
    , sellprice
    , sellprice_netto
    , sellprice_adult 
    , sellprice_child 
    , sellprice_infant 
    , sellprice_senior
    , fee_in_price
    , tiket_comission 
    , tax_percent_in_price 
  from
    `datamart-finance.staging.v_detail__event_connect_ms` decm
)
, event_order as /* use this because commission in oecm is not rounded, but floor-ed, example order id 104001549 */
(
  select
    order_id
    , round(sum(commission)) as commission
  from
    (
      select 
        _id
        , safe_cast(coreOrderId as int64) order_id
        , pt.commissionInCents.numberLong/100 commission
        , rank() over(partition by coreorderId, pt.code order by lastModifiedDate desc) rownum
      from 
        `datamart-finance.staging.v_events_v2_order__order_l2` o
        left join unnest (priceTierQuantities) pt
        left join unnest(tickets) tic on /*to get the same pricetierquantities code as the tickets*/
      lower(tic.priceTierCode) = lower(pt.code)
    )
  where
    rownum = 1
  group by 
    1
)
, oecm as (
  select
    distinct
    order_id
    , order_detail_id
    , event_type
    , ext_source
    , business_id
    , supplier_id
    , event_name
    , event_category
    , is_tiketflexi
    /* , case
      when tiket_comission > 100 then 0
       else round(safe_divide((((qty_adult * sellprice_adult) + (qty_child * sellprice_child) + (qty_infant * sellprice_infant) + (qty_senior * sellprice_senior)) - fee_in_price) * tiket_comission, (100+tax_percent_in_price))) 
       end as commission_event*/
     /*, safe_cast(0 as float64) as commission_event /* requested by Anggi Anggara at 25 April 2020 - Paulus, event commission not calculated in workday*/
     , case 
        when event_category = 'HOTEL' then coalesce(eo.commission,(coalesce(sellprice,0) - coalesce(sellprice_netto,0))*qty_all)
        else 0
       end as commission_event/* requested by Anggi Anggara at 08 Jul 2020 - Paulus, count commission for tiket flexi */
  from
    `datamart-finance.staging.v_order__event_connect_ms`
    left join decm using (detail_event_id)
    left join event_order eo using (order_id) 
)
, fact_event as (
  select
    order_detail_id
    , quantity_event
    , ticket_number_event
    , 0 as subsidy_event
    , 0 as upselling_event
    , commission_event
    , ext_source as ext_source_event
    , case
        when oecm.event_category = 'HOTEL' then 'Hotel'
        when oecm.event_name like ('Airport Transfer%') then 'Car'
        when lower(oecm.event_name) like ('sewa mobil%') and oecm.event_category = 'TRANSPORT' then 'Car' --TTD car
        when oecm.event_name like ('Tix-Spot Airport Lounge%') then 'Others'
        when lower(oecm.event_name) like ('%railink%') then 'Train'
        when oecm.event_type in ('D') then 'Attraction'
        when oecm.event_type in ('E') then 'Activity'
        when oecm.event_type not in ('D','E') then 'Event'
      end as product_category_event
    , case
        when length(business_id) = 0 then '(blank)'
        when business_id is null then '(null)'
        else business_id
      end as product_provider_event
    , case
        when lower(oecm.event_name) like ('%railink%') then 'VR-00000026'
        when length(supplier_id) = 0 then '(blank)'
        when supplier_id is null then '(null)'
        else coalesce(msatl.workday_supplier_reference_id,supplier_id)
      end  as supplier_event
    , case
        when oecm.event_category = 'HOTEL' then 'Hotel_Voucher'
        when oecm.event_name like ('Airport Transfer%') then 'Shuttle'
        when oecm.event_name like ('Tix-Spot Airport Lounge%') then 'Lounge'
        when lower(oecm.event_name) like ('sewa mobil%') and oecm.event_category = 'TRANSPORT' then 'Shuttle' --TTD car
        else 'Ticket'
      end as revenue_category_event
  from 
    oce
    inner join oecm using (order_detail_id)
    left join master_supplier_airport_transfer_and_lounge msatl 
      on msatl.workday_supplier_name = case
                                        when oecm.event_name like ('Airport Transfer%') then 'Airport Transfer'
                                        when oecm.event_name like ('Tix-Spot Airport Lounge%') then 'Tix-Sport Airport Lounge' 
                                      end
)


/* Fact Flight */
, ocf as (
  select 
    order_detail_id
    , string_agg(distinct 
      case
        when ocf.vendor = 'sa' then wsr_name.workday_supplier_reference_id
        when ocf.vendor = 'na' then wsr_id.workday_supplier_reference_id
      end) as supplier_flight
    , string_agg(distinct ticket_status) as ticket_status_flight
    , sum(round(
        case 
          when ocf.airlines_master_id != 20865 then 
            case
              when price_total = 0 
                or price_total is null 
                  then price_adult + price_child + price_infant + baggage_fee
              else price_total
            end 
            + ifnull(sub_price_IDR, 0)
            - ifnull(balance_due, 0)
          else 0
        end
      )) as commission_flight /* Commmission except Citilink*/
    , sum(round(
        case 
          when ocf.airlines_master_id != 20865 then 
            case
              when price_total = 0 
                or price_total is null 
                  then price_adult + price_child + price_infant + baggage_fee
              else price_total
            end 
            + ifnull(sub_price_IDR, 0)
            - ifnull(price_nta, 0)
          else 0
        end
      )) as commission_price_nta_flight /* 13 May 2020, Anggi Anggara: for lion group, start order >= 2020-05-11 using price_nta*//* 27 May 2020, Anggi Anggara: for trigana, sriwjaya , transnusa, start order >= 2020-05-11 using price_nta*/
    , sum(round(
        case 
          when ocf.airlines_master_id = 20865 then 
            case
              when price_total = 0 
                or price_total is null 
                  then price_adult + price_child + price_infant + baggage_fee
              else price_total
            end 
            + ifnull(sub_price_IDR, 0)
            - ifnull(balance_due, 0)
          else 0
        end
      )) as upselling_flight /* Upselling only for flight Citilink*/
    , sum(round(ifnull(balance_due,0) - baggage_fee)) as cogs_flight
    , sum(round(ifnull(price_nta,0) - baggage_fee)) as cogs_price_nta_flight /* 13 May 2020, Anggi Anggara: for lion group, start order >= 2020-05-11 using price_nta*//* 27 May 2020, Anggi Anggara: for trigana, sriwjaya , transnusa, start order >= 2020-05-11 using price_nta*/
    , sum(round(baggage_fee)) as baggage_fee
    , max(count_adult) + max(count_child) + max(count_infant) as quantity_flight
    , string_agg(distinct booking_code) as booking_code_flight
    , sum(sub_price_idr) as subsidy_flight
  from
    `datamart-finance.staging.v_order__cart_flight` ocf
    left join wsr_id using (airlines_master_id,vendor)
    left join wsr_name on ocf.account = wsr_name.supplier_name and ocf.vendor = 'sa'
  where
    departure_time >= (select filter2 from fd)
  group by
    order_detail_id
)
, ocfp as (
  select
    order_detail_id
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
          reschedule_id
          , new_order_id
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
  from
    (
    select
      order_detail_id
      , parent_id
      , total_price
      , updated_at 
      , processed_dttm
      , row_number() over (partition by order_detail_id order by updated_at desc ,processed_dttm desc) rn
    from 
      `datamart-finance.staging.v_order__cart_bundling_addons`
    group by
      1,2,3,4,5
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
, fact_flight as (
  select
    order_detail_id
    , quantity_flight
    , subsidy_flight * -1 as subsidy_flight
    , cogs_flight
    , cogs_price_nta_flight
    , commission_flight
    , commission_price_nta_flight
    , baggage_fee
    , booking_code_flight
    , ticket_number_flight
    , ticket_status_flight
    , supplier_flight
    , upselling_flight
    , halodoc_sell_price_amount
    , halodoc_pax_count
    , is_has_halodoc_flag  
    , halodoc_detail_name
    , 'Ticket' as revenue_category_flight
    , 'Flight' as product_category_flight
  from
    ocf
    left join ocfp using (order_detail_id)
    left join ocba using (order_detail_id)
    left join ocbap using (halodoc_order_detail_id)
    left join ocdba using (halodoc_order_detail_id)
    
)

/* Fact Hotel */
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
    , max(hotel_itineraryNumber) as itinerary_id
    , string_agg(distinct room_source) as room_source
    , string_agg(distinct json_extract_scalar(additional_info,'$.name')) as room_source_info
    , max(created_timestamp) as created_timestamp
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
  distinct(new_order_id) as order_id
  , order_id as old_id_rebooking
  , safe_cast(total_customer_price-new_total_customer_price as float64) as diff_amount_rebooking
  from `datamart-finance.staging.v_order__automatic_rebooking` 
  where rebook_status='SUCCESS'
)
, fact_hotel as (
  select
    order_id
    , itinerary_id
    , quantity as quantity_hotel
    , case
        when room_source = 'TIKET' then hbd.cogs_native
        when room_source LIKE '%AGODA%' then case when ott.rebooking_price_hotel > 0 then ott.rebooking_price_hotel - ott.vendor_incentive_hotel else ott.nett_price - ott.vendor_incentive_hotel end
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
    , 'Hotel' as product_category_hotel
  from
    ott
    left join hb using (itinerary_id)
    left join hbd using (itinerary_id)
    left join hbao_array using (itinerary_id)
    left join hbao_sum using (itinerary_id)
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
    oc.order_id
    , ocd.order_detail_id
    , 'GTN_IDN' as company
    , case
          when date(payment_timestamp) >= '2020-04-06' and ca.corporate_flag is not null and op.payment_source  in ('cash_onsite') then ca.business_id /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when date(payment_timestamp) <'2020-04-06' and ca.corporate_flag is not null then ca.business_id /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when 
            date(payment_timestamp) >= '2020-04-06' and oc.reseller_type in ('none','online_marketing','native_apps')
            and (ca.corporate_flag is null or (ca.corporate_flag is not null and op.payment_source not in ('cash_onsite')))
              then safe_cast(oc.account_id as string)
          when 
            date(payment_timestamp) < '2020-04-06' and oc.reseller_type in ('none','online_marketing','native_apps')
            and (ca.corporate_flag is null)
              then safe_cast(oc.account_id as string)
          when oc.reseller_type in ('none','online_marketing','native_apps') then safe_cast(oc.account_id as string)
          when oc.reseller_type in ('tiket_agent','txtravel','agent','affiliate') then safe_cast(oc.reseller_id as string)
          when oc.reseller_type in ('reseller','widget') then safe_cast(oc.reseller_id as string)
        end as customer_id
    , case
          when date(payment_timestamp) >= '2020-04-06' and ca.corporate_flag is not null and op.payment_source  in ('cash_onsite') then 'B2B Corporate' /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when date(payment_timestamp) <'2020-04-06' and ca.corporate_flag is not null then 'B2B Corporate' /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when 
            date(payment_timestamp) >= '2020-04-06' and oc.reseller_type in ('none','online_marketing','native_apps')
            and (ca.corporate_flag is null or (ca.corporate_flag is not null and op.payment_source not in ('cash_onsite')))
              then 'B2C'
          when 
            date(payment_timestamp) < '2020-04-06' and oc.reseller_type in ('none','online_marketing','native_apps')
            and (ca.corporate_flag is null)
              then 'B2C'
          when oc.reseller_type in ('none','online_marketing','native_apps') then 'B2C'
          when oc.reseller_type in ('tiket_agent','txtravel','agent','affiliate') then 'B2B Offline'
          when oc.reseller_type in ('reseller','widget') then 'B2B Online'
        end as customer_type
    , ocd.selling_currency as selling_currency
    , oc.payment_timestamp
    , date(oc.payment_timestamp) as payment_date
    , ocd.order_type as order_type
    , occ.auth_code as authentication_code
    , onp.virtual_account
    , case
        when ocd.order_type in ('tixhotel','hotel','flight','car','train','event') then trim(ocdgv.giftcard_voucher)
        else null
      end as giftcard_voucher
    , case
        when ocd.order_type in ('tixhotel','hotel','flight','car','train','event') then trim(ocdpc.promocode_name)
        else null
      end as promocode_name
    , op.payment_source as payment_source
    , occ.payment_gateway as payment_gateway
    , replace(coalesce(fpm2.payment_type_bank, fpm.payment_type_bank),' ','_') as payment_type_bank
    , ocdp.payment_order_name as payment_order_name
    , coalesce(
        round(                                                  /* there are cases that if we only use round(payment_charge*selling proportion) then when it summed by order_id */
          case                                                  /* , the summed value will be more than the original value. */
            when ocd.order_detail_id = ocd.last_order_detail_id /* For example, if there are 2 order_detail_id and the proportion is 0.5 and 0.5, the total is 301, */
              then ocdp.payment_charge - sum(                   /* then it will be 151 for each order detail id. When summed by order_id, it will give result 302. */ 
                                          round(                /* Hence, we calculate for the last order_detail_id will be the remain */ 
                                            case                /* of original price minus sum(round(payment_charge * selling_proportion)) of other order_detail_id  */
                                              when ocd.order_detail_id != ocd.last_order_detail_id then ocdp.payment_charge 
                                              else 0 
                                            end * ocd.selling_price_proportion_value)) over(partition by order_id)
            else ocdp.payment_charge * ocd.selling_price_proportion_value
          end
        ,0)
      ,0) as payment_charge
    , coalesce(round( 
        case 
          when ocd.order_detail_id = ocd.last_order_detail_id 
            then ocdcf.convenience_fee_amount - sum(
                                            round(
                                              case 
                                                when ocd.order_detail_id != ocd.last_order_detail_id then ocdcf.convenience_fee_amount 
                                                else 0 
                                              end * ocd.selling_price_proportion_value)) over(partition by order_id)
          else ocdcf.convenience_fee_amount * ocd.selling_price_proportion_value
         end
      ,0),0) as convenience_fee_amount
    , coalesce(round( 
        case 
          when ocd.order_detail_id = ocd.last_order_detail_id 
            then ocdpc.promocode_value - sum(
                                          round(
                                            case 
                                              when ocd.order_detail_id != ocd.last_order_detail_id then ocdpc.promocode_value 
                                              else 0 
                                            end * ocd.selling_price_proportion_value)) over(partition by order_id)
          else ocdpc.promocode_value * ocd.selling_price_proportion_value
         end
      ,0),0) as promocode_value
    , coalesce(round( 
        case 
          when ocd.order_detail_id = ocd.last_order_detail_id 
            then ocdgv.giftvoucher_value - sum(
                                            round(
                                              case 
                                                when ocd.order_detail_id != ocd.last_order_detail_id then ocdgv.giftvoucher_value 
                                                else 0 
                                              end * ocd.selling_price_proportion_value)) over(partition by order_id)
          else ocdgv.giftvoucher_value * ocd.selling_price_proportion_value
         end
      ,0),0) as giftvoucher_value
    , coalesce(round( 
        case 
          when ocd.order_detail_id = ocd.last_order_detail_id 
            then ocdrd.refund_deposit_value - sum(
                                                round(
                                                  case 
                                                    when ocd.order_detail_id != ocd.last_order_detail_id then ocdrd.refund_deposit_value 
                                                    else 0 
                                                  end * ocd.selling_price_proportion_value)) over(partition by order_id)
          else ocdrd.refund_deposit_value * ocd.selling_price_proportion_value
         end
      ,0),0) as refund_deposit_value
    , coalesce(round( 
        case 
          when ocd.order_detail_id = ocd.last_order_detail_id 
            then ocdtp.tiketpoint_value - sum(
                                            round(
                                              case 
                                                when ocd.order_detail_id != ocd.last_order_detail_id then ocdtp.tiketpoint_value 
                                                else 0 
                                              end * ocd.selling_price_proportion_value)) over(partition by order_id)
          else ocdtp.tiketpoint_value * ocd.selling_price_proportion_value
         end
      ,0),0) as tiketpoint_value
    , coalesce(ocdi.insurance_value,0) as insurance_value
    , ocdi.flight_insurance_json
    , coalesce(ocdci.cancel_insurance_value,0) as cancel_insurance_value
    , ocdgv.giftvoucher_name as giftvoucher_name
    , ocdgv.giftcard_voucher_purpose
    , ocdgv.giftcard_voucher_user_email_reference_id
    , ocdrd.refund_deposit_name
    , ocdi.insurance_name
    , ocdci.cancel_insurance_name
    , wmpc.nominal_value
    , wmpc.percentage_value
    , op.payment_amount
    , round(
        coalesce( 
          case 
            when ocd.order_detail_id = ocd.last_order_detail_id 
              then (wmpc.nominal_value + (wmpc.percentage_value*op.payment_amount/100)) - sum(
                                              round(
                                                case 
                                                  when ocd.order_detail_id != ocd.last_order_detail_id then (wmpc.nominal_value + (wmpc.percentage_value*op.payment_amount/100)) 
                                                  else 0 
                                                end * ocd.selling_price_proportion_value)) over(partition by order_id)
            else (wmpc.nominal_value + (wmpc.percentage_value*op.payment_amount/100)) * ocd.selling_price_proportion_value
           end
        ,0)
      ,2) as pg_charge
    , oc.cc_installment
    , ocd.order_name
    , ocd.order_name_detail
    , ocdi.insurance_issue_code
    , occi.cancel_insurance_issue_code
    , occ.acquiring_bank
    , coalesce(
        ft.cogs_train
        , case
            when ocd.order_type = 'car' then ocd.selling_price
            else null
          end
        , case
            when ocd.order_type = 'event' then ocd.selling_price - fe.commission_event
            else null
          end
        , fh.cogs_hotel
        , case 
            when date(oc.payment_timestamp) >= '2020-05-11' and ff.supplier_flight in ('VR-00000006','VR-00000011','VR-00000004','VR-00000007') then ff.cogs_price_nta_flight /* 13 May 2020, Anggi Anggara: for lion group, start order >= 2020-05-11 using price_nta*//* 27 May 2020, Anggi Anggara: for trigana, sriwjaya , transnusa, start order >= 2020-05-11 using price_nta*/
            else ff.cogs_flight
          end
        , fat.cogs_airport_transfer
        , 0
      ) as cogs
    , coalesce(
        ft.quantity_train
        , fc.quantity_car
        , fe.quantity_event
        , ff.quantity_flight
        , fh.quantity_hotel
        , fat.quantity_airport_transfer
        , 0
      ) as quantity
    , coalesce(
        ft.commission_train
        , fc.commission_car
        , fe.commission_event
        , case
            when ocd.order_type = 'tixhotel' then
              case when fh.rebooking_price_hotel > 0 then fh.rebooking_price_hotel - fh.cogs_hotel + (fh.subsidy_hotel*-1) - fh.upselling_hotel - fh.total_add_ons_hotel_sell_price_amount
              else ocd.selling_price - fh.cogs_hotel + (fh.subsidy_hotel*-1) - fh.upselling_hotel - fh.total_add_ons_hotel_sell_price_amount
              end
          end
        , case 
            when date(oc.payment_timestamp) >= '2020-05-11' and ff.supplier_flight in ('VR-00000006','VR-00000011','VR-00000004','VR-00000007') then ff.commission_price_nta_flight /* 13 May 2020, Anggi Anggara: for lion group, start order >= 2020-05-11 using price_nta*//* 27 May 2020, Anggi Anggara: for trigana, sriwjaya , transnusa, start order >= 2020-05-11 using price_nta*/
            else ff.commission_flight
          end
        , 0
      ) as commission
    , coalesce(
        ft.upselling_train
        , fc.upselling_car
        , fe.upselling_event
        , ff.upselling_flight
        , fh.upselling_hotel
        , 0
      ) as upselling
    , coalesce(
        ft.subsidy_train
        , fc.subsidy_car
        , fe.subsidy_event
        , ff.subsidy_flight
        , fh.subsidy_hotel
        , 0
      ) as subsidy
    , coalesce(
        ft.product_category_train
        , fc.product_category_car
        , fe.product_category_event
        , ff.product_category_flight
        , fh.product_category_hotel
        , fat.product_category_airport_transfer
      ) as product_category
    , coalesce(fh.rebooking_price_hotel,0) as rebooking_price_hotel
    , coalesce(ocd.selling_price - fh.rebooking_price_hotel,0) as rebooking_sales_hotel
    , coalesce(
        ft.supplier_train
        , fc.supplier_car
        , coalesce(mes.new_supplier_id,fe.supplier_event) /* include for airport transfer because in one join with master event */
        , ff.supplier_flight
        , fh.supplier_hotel
      ) as supplier
    , coalesce(
        ft.product_provider_train
        , fc.product_provider_car
        , coalesce(mepp.new_product_provider_id,fe.product_provider_event) /* include for airport transfer because in one join with master event */
        , case 
            when ocd.order_type = 'flight' then safe_cast(ocd.order_master_id as string)
            else null 
          end
        , fh.product_provider_hotel
      ) as product_provider
    , coalesce(
        ft.revenue_category_train
        , fc.revenue_category_car
        , fe.revenue_category_event
        , ff.revenue_category_flight
        , fh.revenue_category_hotel
        , fat.revenue_category_airport_transfer
      ) as revenue_category
    , coalesce(
        ft.vat_out_train
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
            when ocd.order_detail_id = ocd.last_order_detail_id 
              then rrbc.reschedule_cashback_amount - sum(
                                              round(
                                                case 
                                                  when ocd.order_detail_id != ocd.last_order_detail_id then rrbc.reschedule_cashback_amount 
                                                  else 0 
                                                end * ocd.selling_price_proportion_value)) over(partition by order_id)
            else rrbc.reschedule_cashback_amount * ocd.selling_price_proportion_value
           end
        ,0)
        , 0
      ) as reschedule_cashback_amount
    , coalesce(
        round( 
          case 
            when ocd.order_detail_id = ocd.last_order_detail_id 
              then rrbc.reschedule_promocode_amount - sum(
                                              round(
                                                case 
                                                  when ocd.order_detail_id != ocd.last_order_detail_id then rrbc.reschedule_promocode_amount 
                                                  else 0 
                                                end * ocd.selling_price_proportion_value)) over(partition by order_id)
            else rrbc.reschedule_promocode_amount * ocd.selling_price_proportion_value
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
            when ocd.order_detail_id = ocd.last_order_detail_id 
              then rrbc.refund_amount_flight - sum(
                                              round(
                                                case 
                                                  when ocd.order_detail_id != ocd.last_order_detail_id then rrbc.refund_amount_flight 
                                                  else 0 
                                                end * ocd.selling_price_proportion_value)) over(partition by order_id)
            else rrbc.refund_amount_flight * ocd.selling_price_proportion_value
           end
        ,0)
        , 0
      ) as refund_amount_flight
    , coalesce(
        round( 
        case 
          when ocd.order_detail_id = ocd.last_order_detail_id 
            then ocdrd.refund_deposit_value - sum(
                                                round(
                                                  case 
                                                    when ocd.order_detail_id != ocd.last_order_detail_id then ocdrd.refund_deposit_value 
                                                    else 0 
                                                  end * ocd.selling_price_proportion_value)) over(partition by order_id)
          else ocdrd.refund_deposit_value * ocd.selling_price_proportion_value
         end
      ,0)
      * -1 
      - round( 
          case 
            when ocd.order_detail_id = ocd.last_order_detail_id 
              then rrbc.refund_amount_flight - sum(
                                              round(
                                                case 
                                                  when ocd.order_detail_id != ocd.last_order_detail_id then rrbc.refund_amount_flight 
                                                  else 0 
                                                end * ocd.selling_price_proportion_value)) over(partition by order_id)
            else rrbc.refund_amount_flight * ocd.selling_price_proportion_value
           end
        ,0)
      ,0) as reschedule_miscellaneous_amount
    , coalesce(
        ft.booking_code_train
        , ff.booking_code_flight
        , fh.itinerary_id
      ) as booking_code
    , coalesce(
        fe.ticket_number_event
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
        when ocd.order_type in ('flight','train') then concat(safe_cast(oc.order_id as string), ' - ', ocd.order_name_detail, ' / ', ocd.order_name)
        when ocd.order_type in ('tixhotel','event') then concat(safe_cast(oc.order_id as string), ' - ', ocd.order_name, ' / ', ocd.order_name_detail)
        when ocd.order_type in ('car') then concat(safe_cast(oc.order_id as string), ' - ', ocd.order_name_detail)
        when order_type = 'airport_transfer' then concat(safe_cast(oc.order_id as string), ' - ', order_name_detail, ' - ', zone_airport_transfer)
      end as memo_product
    , case 
        when ocd.order_type in ('tixhotel') then concat(safe_cast(oc.order_id as string), ' - ', ocd.order_name, ' / ', ocd.order_name_detail, ' - ', fh.itinerary_id)
      end as memo_hotel
    , case 
        when ocd.order_type in ('flight') then concat(safe_cast(oc.order_id as string), ' / ', ff.booking_code_flight, ' - ', ocd.order_name_detail, ' / ', ocd.order_name)
      end as memo_flight
    , case
        when cancel_insurance_value is not null then concat(safe_cast(oc.order_id as string), ' - ', 'Cermati Anti Galau, Issue Code: ', ifnull(occi.cancel_insurance_issue_code,'')) 
        else null
      end as memo_cancel_insurance
    /*, case
        when insurance_value is not null then concat(safe_cast(oc.order_id as string), ' - ', 'Cermati BCA Insurance, Issue Code: ', ifnull(oci.insurance_issue_code,'')) 
        else null
      end as memo_insurance */ /*move memo_insurance to flight_insurance_array to enhance the query for multi_insurance*/
    , safe_cast(null as string) as memo_insurance
    , case 
        when is_has_halodoc_flag = 1 then concat(safe_cast(oc.order_id as string), ' - ', halodoc_detail_name)
        else null
      end as memo_halodoc
    , case 
        when convenience_fee_amount > 0 then concat(safe_cast(oc.order_id as string), ' - ', convenience_fee_order_name)
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
        when ocd.order_type not in ('flight','train','tixhotel') then 'issued'
        when ocd.order_type in ('flight','train') and coalesce(ff.ticket_status_flight, ft.ticket_status_train) = 'issued' then 'issued'
        when ocd.order_type in ('tixhotel') and fh.is_hotel_issued_flag = 1 then 'issued'
        else 'not issued'
      end as issued_status
    , ocd.selling_price_proportion_value
    , case 
        when 
          string_agg(case
            when ocd.order_type not in ('flight','train','tixhotel') then 'issued'
            when ocd.order_type in ('flight','train') and coalesce(ff.ticket_status_flight, ft.ticket_status_train) = 'issued' then 'issued'
            when ocd.order_type in ('tixhotel') and fh.is_hotel_issued_flag = 1 then 'issued'
            else 'not issued'
          end) over(partition by order_id) like '%not_issued%' 
        then 0
        else 1
      end as all_issued_flag
    , case
        when 
          ocd.order_type = 'event' 
          and 
            (
              (
                coalesce(mepp.new_product_provider_id,fe.product_provider_event) = '(null)'
                or safe_cast(coalesce(mepp.new_product_provider_id,fe.product_provider_event) as string) = '0' 
                or safe_cast(coalesce(mepp.new_product_provider_id,fe.product_provider_event) as string) = '-'
                or safe_cast(coalesce(mepp.new_product_provider_id,fe.product_provider_event) as string) = '(blank)'
                or coalesce(mes.new_supplier_id,fe.supplier_event) = '(null)'
                or safe_cast(coalesce(mes.new_supplier_id,fe.supplier_event) as string) = '0' 
                or safe_cast(coalesce(mes.new_supplier_id,fe.supplier_event) as string) = '-'
                or safe_cast(coalesce(mes.new_supplier_id,fe.supplier_event) as string) = '(blank)'
              )
            or
              (
                date(oc.payment_timestamp) = mes.start_date
                or date(oc.payment_timestamp) = mepp.start_date
              )
            )
        then 1
        when 
          ocd.order_type = 'airport_transfer'
          and (mes.new_supplier_id is null or mepp.new_product_provider_id is null)
        then 1
        else 0
      end as event_data_error_flag
    , case
        when ocd.order_type = 'tixhotel' and fh.hotel_payment_type like '%pay_at_hotel%' then 1
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
  from
    oc
    inner join ocd using (order_id)
    inner join op using (order_id)
    left join ocdp using (order_id)
    left join ocdpc using (order_id)
    left join ocdgv using (order_id)
    left join ocdrd using (order_id)
    left join ocdtp using (order_id)
    left join ocdi using (order_id,order_detail_id)
    left join ocdci using (order_id,order_detail_id)
    left join ocdcf using (order_id)
    /* left join oci using (insurance_order_detail_id) */ /* this table moved to ocdi for multi insurance*/
    left join occi using (cancel_insurance_order_detail_id)
    left join ma using (account_id)
    left join ca using (account_username)
    left join occ using (order_id)
    left join onp using (order_id)
    left join fpm2 using (payment_gateway,acquiring_bank,payment_source)
    left join fpm using (payment_source)
    left join fact_train ft using (order_detail_id)
    left join fact_car fc using (order_detail_id)
    left join fact_event fe using (order_detail_id)
    left join fact_flight ff using (order_detail_id)
    left join fact_hotel fh using (order_id)
    left join fact_airport_transfer fat using (order_detail_id)
    left join ores using (order_id)
    left join rrbc using (order_id, reschedule_passenger_id)
    left join ocf_reschedule using (flight_reschedule_old_order_detail_id)
    left join wmpc on wmpc.payment_type_bank = replace(coalesce(fpm2.payment_type_bank, fpm.payment_type_bank),' ','_') and wmpc.installment = oc.cc_installment and date(oc.payment_timestamp) between wmpc.start_date and coalesce(wmpc.end_date,current_date())
    left join master_event_supplier mes on (coalesce(fe.supplier_event,fat.old_supplier_id) = mes.old_supplier_id and ocd.order_name = mes.event_name)
    left join master_event_product_provider mepp on (coalesce(fe.product_provider_event,fat.old_product_provider_id) = mepp.old_product_provider_id and ocd.order_name = mepp.event_name)
    left join oar using (order_id)

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
            when mbo.active_date >= c.payment_date or mbo.active_date is null then 1
            else 0
          end
        else 0
      end as new_b2b_online_and_offline_flag
    , case
        when c.customer_type = 'B2C Corporate' then
          case
            when mbc.active_date >= c.payment_date or mbc.active_date is null then 1
            else 0
          end
        else 0
      end as new_b2b_corporate_flag
    , case
        when c.product_category = 'Flight' and sum(case when c.supplier is null then 1 else 0 end) over(partition by order_id) > 0 then 1
        else 0
      end as is_supplier_flight_not_found_flag
    , case 
        when sum(cogs + commission + upselling + subsidy + payment_charge + promocode_value + giftvoucher_value + refund_deposit_value + tiketpoint_value + insurance_value + cancel_insurance_value + ifnull(vat_out,0) + ifnull(baggage_fee,0) + rebooking_sales_hotel + total_add_ons_hotel_sell_price_amount + halodoc_sell_price_amount + convenience_fee_amount + diff_amount_rebooking) over(partition by order_id) > 0 
        and sum(cogs + commission + upselling + subsidy + payment_charge + promocode_value + giftvoucher_value + refund_deposit_value + tiketpoint_value + insurance_value + cancel_insurance_value + ifnull(vat_out,0) + ifnull(baggage_fee,0) + rebooking_sales_hotel + total_add_ons_hotel_sell_price_amount + halodoc_sell_price_amount + convenience_fee_amount + diff_amount_rebooking) over(partition by order_id) <> payment_amount
          then 0
        else 1
      end as is_amount_valid_flag
  from
    combine c
    left join master_supplier ms on ms.supplier_id = c.supplier and c.payment_date >= ms.start_date and c.payment_date < ms.end_date
    left join master_product_provider mpp on mpp.product_provider_id = c.product_provider and c.payment_date >= mpp.start_date and c.payment_date < mpp.end_date
    left join master_b2b_online_and_offline mbo on safe_cast(mbo.business_id as string) = c.customer_id and c.payment_date >= mbo.start_date and c.payment_date < mbo.end_date
    left join master_b2b_corporate mbc on mbc.business_id = c.customer_id and c.payment_date >= mbc.start_date and c.payment_date < mbc.end_date
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
where
  tr.order_id is null
)

select * from append