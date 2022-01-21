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
    and safe_cast(Delete_Flag as date) is null
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
    account_id
    , account_username
  from (
    select
      *
      , row_number() over(partition by account_id order by processed_dttm desc, accountlastlogin desc) as rn
    from
      ma_ori
  ) 
  where rn = 1
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
    payment_status in ( 'paid','discarded')
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
        else payment_amount + coalesce(extra_fee,0)
      end as payment_amount
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
, hbi as (
  select
    order_id
    , itinerary_id
    , string_agg(insurance_package) as insurance_package
    , safe_cast(sum(premium_total) as float64) as premium
    , concat(
        case when string_agg(insurance_package) is not null then
        string_agg
        (
          concat
          (
            '{"order_detail_id":"",'
            ,'"insurance_value":"',coalesce(safe_cast(premium_total as string),'No Insurance Value Set'),'",'
            ,'"insurance_name":"',coalesce(insurance_package, 'No Insurance Name Set'),'",'
            ,'"insurance_issue_code":"",'
            ,'"memo_insurance":"',order_id,' - NHA Insurance"}'
          )
        ) else '' end
      ) as insurance_json
  from
    `datamart-finance.staging.v_homes_booking_insurances`
    left join
      (
        select
          distinct
          safe_cast(hotel_itinerarynumber as int64) as itinerary_id
          , order_id
        from
          `datamart-finance.staging.v_order__tixhotel`
        where
          created_timestamp >= (select filter2 from fd)
      ) using (itinerary_id)
  where
    premium_total <> 0
    and not is_deleted
    and booking_status = 'issued'
  group by
  1,2
)
, ocdi as ( /* 6 Sept 2020: Change query for multi insurance. All the insurances will be in the array, hence the table's behavior will be change since the deployment of this query.*/
  select
    order_id 
    , coalesce(safe_cast(split(split(order_name_detail,'#') [safe_offset(1)],' ') [safe_offset(0)] as int64), parent_id) as order_detail_id
    , safe_cast(sum(customer_price) as float64) as insurance_value
    , string_agg(order_name order by order_detail_id) as insurance_name
    , string_agg(insurance_issue_code order by order_detail_id) as insurance_issue_code
    , concat(string_agg(concat('{"order_detail_id":"',order_detail_id,'",'
      ,'"insurance_value":"',coalesce(safe_cast(customer_price as string),'No Insurance Value Set'),'",'
      ,'"insurance_name":"',coalesce(order_name, 'No Insurance Name Set'),'",'
      ,'"insurance_issue_code":"',coalesce(safe_cast(insurance_issue_code as string),'Insurance not issued'),'",'
      ,'"memo_insurance":"',concat(safe_cast(order_id as string), ' - ', 'Cermati BCA Insurance, Issue Code: ', ifnull(insurance_issue_code,''))
      ,'"}') order by order_detail_id asc)) as flight_insurance_json
  from
    ocd_or
    left join oci using (order_detail_id)
  where
    order_type in ('insurance')
  group by 1,2
)
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
    , concat(string_agg(concat('{"order_detail_id":"',order_detail_id,'",'
      ,'"insurance_value":"',coalesce(safe_cast(customer_price as string),'No Insurance Value Set'),'",'
      ,'"insurance_name":"',coalesce(order_name, 'No Insurance Name Set'),'",'
      ,'"insurance_issue_code":"',coalesce(safe_cast(insurance_issue_code as string),'Insurance not issued'),'",'
      ,'"memo_insurance":"',concat(safe_cast(order_id as string), ' - ', 'Cermati BCA Insurance, Issue Code: ', ifnull(insurance_issue_code,''))
      ,'"}') order by order_detail_id asc)) as flight_insurance_json
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
, combine as (
  select
    * except
      (
        payment_type_bank
        , insurance_value
        , insurance_name
        , insurance_issue_code
        , flight_insurance_json
        , business_id
        , corporate_flag
      )
    , coalesce(ca.business_id, corp.business_id) as business_id
    , coalesce(ca.corporate_flag, corp.corporate_flag) as corporate_flag
    , coalesce(ocdi.insurance_value, ocdhi.insurance_value) as insurance_value
    , coalesce(ocdi.insurance_name, ocdhi.insurance_name) as insurance_name
    , coalesce(ocdi.insurance_issue_code, ocdhi.insurance_issue_code) as insurance_issue_code
    , coalesce(ocdi.flight_insurance_json, ocdhi.flight_insurance_json) as flight_insurance_json
    , replace(coalesce(fpm2.payment_type_bank, fpm.payment_type_bank),' ','_') as payment_type_bank
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
    left join occi using (cancel_insurance_order_detail_id)
    left join ma using (account_id)
    left join ca using (account_username)
    left join corp using (account_username)
    left join occ using (order_id)
    left join onp using (order_id)
    left join fpm2 using (payment_gateway,acquiring_bank,payment_source)
    left join fpm using (payment_source)
    left join wmpc on wmpc.payment_type_bank = replace(coalesce(fpm2.payment_type_bank, fpm.payment_type_bank),' ','_') and wmpc.installment = oc.cc_installment and date(oc.payment_timestamp) between wmpc.start_date and coalesce(wmpc.end_date,current_date())
    left join hbi using (order_id)
)
/* save the result of this query to temporary table -> let's agree the temporary location will be in `datamart-finance.datasource_workday.temp_customer_invoice_raw_part_1`*/
select
  *
from combine