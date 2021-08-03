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
    --and Start_Date <='2021-03-31' -- is not null /* 30 Maret 2021, temporary solution for b2b corp group*/
    and safe_cast(Delete_Flag as date) is null /*add filter when generate CI > 1 April 2021*/
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
/*
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
*/
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
/*
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
*/
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
    payment_status IN( 'paid','discarded') /*add status discarded for TTD order that deleted by cust in the same day|18 feb 21*/
    and payment_timestamp >= (select filter1 from fd)
    and payment_timestamp < (select filter3 from fd)
)
, op as (
  select
    distinct
    order_id
    , payment_source
    , case
        when
          payment_id = 1
          and data_source = 'spanner'
          and payment_source = 'refund_deposit'
          and is_main_payment
          and
            payment_amount
            -
            coalesce
              (
                safe_cast(trim(regexp_replace(payment_name_detail,'[^0-9 ]','')) as int64)
                , 0
              )
            < 0
          then
            payment_amount
            -
            coalesce
              (
                safe_cast(trim(regexp_replace(payment_name_detail,'[^0-9 ]','')) as int64)
                , 0
              )
        when
          payment_id = 1
          and data_source = 'spanner'
          and payment_source = 'refund_deposit'
          and is_main_payment
          and
            payment_amount
            -
            coalesce
              (
                safe_cast(trim(regexp_replace(payment_name_detail,'[^0-9 ]','')) as int64)
                , 0
              )
            <= 10000
          then 0
        when lower(payment_source)='pay_later' and extra_fee > 0 then payment_amount
        else payment_amount + coalesce(extra_fee,0) end as payment_amount 
  from
    `datamart-finance.staging.v_order__payment`
  where
    payment_id = 1
    and payment_flag = 1
    and payment_timestamp >= (select filter2 from fd)
    and payment_timestamp < (select filter3 from fd)
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
    order_type in ('flight','car','hotel','tixhotel','train','event', 'airport_transfer','railink')
)
, ocdv as (
  select
    distinct
    order_id
    , order_type
  from
    ocd_or
  where
    order_type in ('flight','car','hotel','tixhotel','train','event', 'airport_transfer','railink')
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
  union distinct
  select
    order_id 
    , string_agg(order_name) as payment_order_name
    , safe_cast(sum(extra_fee) as float64) as payment_charge
  from
    ops
  where is_main_payment
  group by 1
)
, ocdgv as (
  select
    order_id 
    , string_agg(distinct order_name) as giftvoucher_name
    , safe_cast(sum(customer_price) as float64) as giftvoucher_value
    , string_agg(distinct replace(tix_gcc.giftcard_voucher,'"','')) as giftcard_voucher
    , string_agg(distinct giftcard_voucher_purpose) as giftcard_voucher_purpose
    , string_agg(distinct giftcard_voucher_user_email_reference_id) as giftcard_voucher_user_email_reference_id
  from
    ocd_or
    --left join gcc on ocd_or.order_detail_id = gcc.order_detail_id_gf
    left join tix_gcc using (order_id)
  where
    order_type in ('giftcard')
  group by 1
  union distinct
  select
    order_id 
    , string_agg(distinct order_name) as giftvoucher_name
    , safe_cast(sum(customer_price*-1) as float64) as giftvoucher_value
    , string_agg(giftcard_voucher) as giftcard_voucher
    , string_agg(distinct giftcard_voucher_purpose) as giftcard_voucher_purpose
    , string_agg(distinct giftcard_voucher_user_email_reference_id) as giftcard_voucher_user_email_reference_id
  from
    ops
    left join tix_gcc using (order_id)
  where order_type = 'giftcard'
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
  union distinct
  select
    order_id
    , string_agg(distinct order_name) as promocode_name
    , safe_cast(sum(customer_price*-1) as float64) as promocode_value
  from
    ops
  where order_type = 'promocode'
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
  union distinct
  select
    order_id 
    , safe_cast(sum(customer_price*-1) as float64) as tiketpoint_value
  from
    ops
  where order_type = 'tiketpoint'
  group by 1
)
, ocdrd as (
  select
    order_id 
    , string_agg(distinct order_name) as refund_deposit_name
    , safe_cast(sum(customer_price) as float64) as refund_deposit_value
    , case
        when order_id is null then 0
        else 1
        end as is_reschedule
  from
    ocd_or
  where
    order_type = 'refund_deposit'
  group by 1
  union distinct
  select
    order_id 
    , string_agg(distinct order_name) as refund_deposit_name
    , coalesce
        (
          - 1
          * safe_cast
            (
              trim(regexp_replace(order_name_detail,'[^0-9 ]',''))
              as float64
            )
          , 0
        )
      as refund_deposit_value
    , case
        when order_id is null then 0
        else 1
        end as is_reschedule
  from
    ops
  where order_type = 'refund_deposit'
  group by 1,order_name_detail
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
    , coalesce(safe_cast(split(split(order_name_detail,'#') [safe_offset(1)],' ') [safe_offset(0)] as int64), parent_id) as order_detail_id
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
/*new datasource for insurance hotel: 17 Mei 2021 - EDP*/
, hi as (
  select
    safe_cast(product.element.transactionDetailId._numberLong as int64) as order_master_id
    , product.element.subscriptionCode as insurance_issue_code
  from 
    `datamart-finance.staging.v_hotel_cart_book` as hcb
    left join unnest(insurance_insuranceDetails.list) as details
    left join unnest(details.element.products.list) as product
  where
    product.element.insuranceType is not null
    and status in ('PAID','REFUNDED')
    and product.element.transactionDetailId._numberLong is not null
    and createdDate >= (select filter2 from fd)
    and createdDate < (select filter3 from fd)
)
, ocdhi as (
  select
    order_id 
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
    join hi using (order_master_id)
  where
    order_type in ('insurance')
  group by 1
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
            /*and (status LIKE '%ISSUED%' or status = 'PAID' or status = 'BLOCKED')*/
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
        when product_category = 'event' then 'Event'
        when product_category = 'transport' and product_supplier = 'Railink' then 'Train' 
        when product_category = 'transport'
          and 
          (
            product_subcategory like '%lepas kunci%'
            or
            product_subcategory like '%city to city%'
            or
            product_subcategory like '%airport%'
          )
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
        when product_category = 'transport' and product_subcategory like '%lepas kunci%' then 'Rental'
        when product_category = 'transport' and 
          (
            product_subcategory like '%airport%'
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
  --where discount_type = 2 /* 20 maret 2021, 2=diskon , 3= Basic Blue Discount*/
  where customer_price*-1 > 1000 /* 28 April 2021, discount < 1000 has reduced balance_due, sample id: 114283044*/
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
          when ocf.airlines_master_id not IN(20865,115334) then 
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
      )) as commission_flight /* Commmission except Citilink*/ /* Add Garuda since 08 Feb 2021*/
    , sum(round(
        case 
          when ocf.airlines_master_id not IN(20865,115334) then 
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
          when ocf.airlines_master_id IN(20865,115334) then 
            case
              when price_total = 0 
                or price_total is null 
                  then price_adult + price_child + price_infant + baggage_fee
              else price_total
            end 
            + ifnull(sub_price_IDR, 0)
           /* - ifnull (discount_amount, 0) since otw campaign balance due has exclude discount subsidy*/ /* 25 maret 2021, price total has reduced by discount member, so we have to add discount member so that upselling value not minus*/
            - ifnull(balance_due, 0)
          else 0
        end
      )) as upselling_flight /* Upselling only for flight Citilink*/ /* Add Garuda since 08 Feb 2021*/
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
    left join ocdis using (order_detail_id) /* 25 maret 2021, join ocdis to get discount member */
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
          --reschedule_id
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
/*, ocfs as (
  select 
    * except (rn)
  from
    (
      select 
        order_detail_id
        , safe_cast(json_extract_scalar(fij,'$.flexi') as bool) as is_flexi
        , row_number() over(partition by order_detail_id) as rn
      from 
        `datamart-finance.staging.v_order__cart_flight_segment`
      left join
          unnest (json_extract_array(flight_info_json)) as fij
      where
        departure_time >= (select filter1 from fd)
        and flight_date >= (select date_add((select date(filter1) from fd), interval 1 day))  
    )
  where rn = 1
)*/
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
     , string_agg(distinct desc_addons) as desc_addons
  from
    (
    select
      order_detail_id
      , parent_id
      , total_price
      , updated_at 
      , processed_dttm
      , json_extract_scalar(param_json,'$[0].description.') as desc_addons
      , row_number() over (partition by order_detail_id order by updated_at desc ,processed_dttm desc) rn
    from 
      `datamart-finance.staging.v_order__cart_bundling_addons`
    group by
      1,2,3,4,5,6
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
    from
      `datamart-finance.staging.v_flight_management_dashboard` 
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
    , booking_code_flight
    , ticket_number_flight
    , ticket_status_flight
    , supplier_flight
    , upselling_flight
    , halodoc_sell_price_amount
    , halodoc_pax_count
    , is_has_halodoc_flag  
    , halodoc_detail_name
    , desc_addons
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
    , max(hotel_itineraryNumber) as itinerary_id
    , string_agg(distinct room_source) as room_source
    , string_agg(distinct json_extract_scalar(additional_info,'$.name')) as room_source_info
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
    distinct(new_order_id) as order_id
    , order_id as old_id_rebooking
    , safe_cast(total_customer_price-new_total_customer_price as float64) as diff_amount_rebooking
  from
    `datamart-finance.staging.v_order__automatic_rebooking` 
  where rebook_status='SUCCESS'
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
    oc.order_id
    , ocd.order_detail_id
    , case
          when date(payment_timestamp) >= '2021-03-01' 
          and reseller_id IN(34382690,34423384) /*24 March 21, add new business id Agoda non login (34423384) - EDP*/
        then 'GTN_SGP'
      else 'GTN_IDN' end as company
    , case
          when date(payment_timestamp) >= '2020-04-06' and ca.corporate_flag is not null and op.payment_source  in ('cash_onsite','corporate_deposit','corporate_invoice','corporate_billing') then ca.business_id /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when date(payment_timestamp) <'2020-04-06' and ca.corporate_flag is not null then ca.business_id /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/ /*2021=07-18 - adding new payment source for b2b corporate*/
          when 
            date(payment_timestamp) >= '2020-04-06' and oc.reseller_type in ('none','online_marketing','native_apps')
            and (ca.corporate_flag is null or (ca.corporate_flag is not null and op.payment_source not in ('cash_onsite','corporate_deposit','corporate_invoice','corporate_billing')))
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
          when date(payment_timestamp) >= '2020-04-06' and ca.corporate_flag is not null and op.payment_source  in ('cash_onsite','corporate_deposit','corporate_invoice','corporate_billing') then 'B2B Corporate' /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when date(payment_timestamp) <'2020-04-06' and ca.corporate_flag is not null then 'B2B Corporate' /* 2020 04 06 - additional request for B2B Corporate only payment source cash_onsite*/
          when 
            date(payment_timestamp) >= '2020-04-06' and oc.reseller_type in ('none','online_marketing','native_apps')
            and (ca.corporate_flag is null or (ca.corporate_flag is not null and op.payment_source not in ('cash_onsite','corporate_deposit','corporate_invoice','corporate_billing')))
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
        when ocd.order_type in ('tixhotel','hotel','flight','car','train','event','airport_transfer','railink') then trim(ocdgv.giftcard_voucher)
        else null
      end as giftcard_voucher
    , case
        when ocd.order_type in ('tixhotel','hotel','flight','car','train','event','airport_transfer','railink') then trim(ocdpc.promocode_name)
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
    , coalesce(ocdi.insurance_value,ocdhi.insurance_value,0) as insurance_value
    , coalesce(ocdi.flight_insurance_json, ocdhi.flight_insurance_json) as flight_insurance_json
    , coalesce(ocdci.cancel_insurance_value,0) as cancel_insurance_value
    , ocdgv.giftvoucher_name as giftvoucher_name
    , ocdgv.giftcard_voucher_purpose
    , ocdgv.giftcard_voucher_user_email_reference_id
    , ocdrd.refund_deposit_name
    , coalesce(ocdi.insurance_name, ocdhi.insurance_name) as insurance_name
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
    , coalesce(ocdi.insurance_issue_code,ocdhi.insurance_issue_code) as insurance_issue_code
    , occi.cancel_insurance_issue_code
    , occ.acquiring_bank
    , coalesce(
        ft.cogs_train
        , fr.cogs_railink
        , case
            when ocd.order_type = 'car' then ocd.selling_price
            else null
          end
        , case
            when ocd.order_type = 'event' then ocd.selling_price - fttd.commission_event - subsidy_event - upselling_event
            else null
          end
        , fh.cogs_hotel
        , case 
            when date(oc.payment_timestamp) >= '2020-05-11' and ff.supplier_flight in ('VR-00000006','VR-00000011','VR-00000004','VR-00017129') then ff.cogs_price_nta_flight /* 13 May 2020, Anggi Anggara: for lion group, start order >= 2020-05-11 using price_nta*//* 27 May 2020, Anggi Anggara: for trigana, sriwjaya , transnusa, start order >= 2020-05-11 using price_nta*/
            when date(oc.payment_timestamp) >= '2021-02-20' and order_flight_commission > 0 and ff.supplier_flight in ('VR-00000003') and manual_markup_amount > 0 then ff.cogs_flight /* 20 feb 2021, for sabre with markup amount > 0 */
            when date(oc.payment_timestamp) >= '2020-10-01' and order_flight_commission > 0 and ff.supplier_flight in ('VR-00000003','VR-00000007','VR-00000012') and ocdrd.is_reschedule is null then ff.cogs_flight - order_flight_commission /* 1 oct 2020, for sabre, transnusa, express only */
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
            when ocd.order_type = 'tixhotel' and oc.reseller_type in ('reseller','widget') and reseller_id = 34272813
              then round(fh.commission_percentage*fh.selling_price/100) /* 10 MARCH 2021, breakdwon partner commission for CTRIP*/
            when ocd.order_type = 'tixhotel' then
              case when fh.rebooking_price_hotel > 0 then fh.rebooking_price_hotel - fh.cogs_hotel + (fh.subsidy_hotel*-1) - fh.upselling_hotel - fh.total_add_ons_hotel_sell_price_amount
              else ocd.selling_price - fh.cogs_hotel + (fh.subsidy_hotel*-1) - fh.upselling_hotel - fh.total_add_ons_hotel_sell_price_amount
              end
          end
        , case 
            when date(oc.payment_timestamp) >= '2020-05-11' and ff.supplier_flight in ('VR-00000006','VR-00000011','VR-00000004','VR-00017129') then ff.commission_price_nta_flight /* 13 May 2020, Anggi Anggara: for lion group, start order >= 2020-05-11 using price_nta*//* 27 May 2020, Anggi Anggara: for trigana, sriwjaya , transnusa, start order >= 2020-05-11 using price_nta*/
             when date(oc.payment_timestamp) >= '2021-02-20' and order_flight_commission > 0 and ff.supplier_flight in ('VR-00000003') and manual_markup_amount > 0 then commission_flight_fmd /* 20 feb 2021, for sabre with markup amount > 0 */
            when date(oc.payment_timestamp) >= '2020-10-01' and order_flight_commission > 0 and ff.supplier_flight in ('VR-00000003','VR-00000007','VR-00000012') and ocdrd.is_reschedule is null then order_flight_commission /* 1 oct 2020, for sabre, transnusa, express only */
            else ff.commission_flight
          end
        , 0
      ) as commission
    , coalesce(
        case
          when ocd.order_type = 'tixhotel' and oc.reseller_type in ('reseller','widget') and reseller_id = 34272813
            then -1*round(fh.cogs_hotel+(fh.commission_percentage*fh.selling_price/100)-ocd.customer_price+fh.subsidy_hotel+fh.upselling_hotel)
            else 0
          end
        , 0
      ) as partner_commission  
    , coalesce(
        ft.upselling_train
        , fr.upselling_railink
        , fc.upselling_car
        , fttd.upselling_event
        , ff.upselling_flight
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
    --, coalesce(ocd.selling_price - fh.rebooking_price_hotel,0) as rebooking_sales_hotel
    , case
        when fh.rebooking_price_hotel>0 then ocd.selling_price - fh.rebooking_price_hotel
        else 0
      end as rebooking_sales_hotel
    , coalesce(
        ft.supplier_train
        , fr.supplier_railink
        , fc.supplier_car
        , coalesce(mes.new_supplier_id,fttd.supplier_event) /* include for airport transfer because in one join with master event */
        , ff.supplier_flight
        , case
            when oc.reseller_type in ('reseller','widget')
              and date(payment_timestamp) >= '2021-03-01' 
              and reseller_id IN(34382690,34423384) /*24 March 21, add new business id Agoda non login (34423384) - EDP*/
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
            when ocd.order_type = 'flight' then safe_cast(ocd.order_master_id as string)
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
        when ocd.order_type in ('flight','train','railink') then concat(safe_cast(oc.order_id as string), ' - ', ocd.order_name_detail, ' / ', ocd.order_name)
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
        when is_has_halodoc_flag = 1 then concat(safe_cast(oc.order_id as string), ' - ', desc_addons)
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
        when ocd.order_type not in ('flight','train','tixhotel','railink') then 'issued'
        when ocd.order_type in ('flight','train','railink') and coalesce(ff.ticket_status_flight, ft.ticket_status_train, fr.ticket_status_railink) = 'issued' then 'issued'
        when ocd.order_type in ('tixhotel') and fh.is_hotel_issued_flag = 1 then 'issued'
        else 'not issued'
      end as issued_status
    , ocd.selling_price_proportion_value
    , case 
        when 
          string_agg(case
            when ocd.order_type not in ('flight','train','tixhotel','railink') then 'issued'
            when ocd.order_type in ('flight','train','railink') and coalesce(ff.ticket_status_flight, ft.ticket_status_train, fr.ticket_status_railink) = 'issued' then 'issued'
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
    , case
        when ocdrd.is_reschedule = 1 and tfrro.is_flexi then true
        else false
        end as is_flexi_reschedule
    , safe_cast(coalesce(tfrro.fare_diff+tfrro.tax_diff+tfrro.reschedule_fee, 0) as float64) flexi_fare_diff
    , safe_cast(0 as float64) flexi_reschedule_fee --bypass increasing flexi fare diff
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
    left join ocdhi using (order_id)
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
    left join fact_railink fr using (order_detail_id)
    left join fact_car fc using (order_detail_id)
    /*left join fact_event fe using (order_detail_id)*/
    left join fact_ttd fttd using (order_id)
    left join fact_flight ff using (order_detail_id)
    left join fact_hotel fh using (order_id)
    left join fact_airport_transfer fat using (order_detail_id)
    left join ores using (order_id)
    /*left join ocfs using (order_detail_id)*/
    left join tfrro on ocd.order_detail_id = tfrro.new_order_detail_id
    left join rrbc using (order_id, reschedule_passenger_id)
    left join ocf_reschedule using (flight_reschedule_old_order_detail_id)
    left join wmpc on wmpc.payment_type_bank = replace(coalesce(fpm2.payment_type_bank, fpm.payment_type_bank),' ','_') and wmpc.installment = oc.cc_installment and date(oc.payment_timestamp) between wmpc.start_date and coalesce(wmpc.end_date,current_date())
    left join master_event_supplier mes on (coalesce(fttd.supplier_event,fat.old_supplier_id) = mes.old_supplier_id and ocd.order_name = mes.event_name)
    left join master_event_product_provider mepp on (coalesce(fttd.product_provider_event,fat.old_product_provider_id) = mepp.old_product_provider_id and ocd.order_name = mepp.event_name)
    left join oar using (order_id)
    left join fmd  using (order_detail_id)
)
/* save the result of this query to temporary table -> let's agree the temporary location will be in `datamart-finance.datasource_workday.temp_customer_invoice_raw_part_1`*/
select
  *
from combine