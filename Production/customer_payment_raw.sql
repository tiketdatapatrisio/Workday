with
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -365 day) as filter2
    , timestamp_add(filter1, interval -366 day) as filter3
    , timestamp_add(filter1, interval 10 day) as filter4
  from
  (
    select
       timestamp_add(timestamp(date(current_timestamp(),'Asia/Jakarta')), interval -247 hour) as filter1
  )
)
, oc as (
  select
    order_id
    , timestamp(datetime(max(payment_timestamp), 'Asia/Jakarta')) as transaction_timestamp
    , max(cc_installment) as cc_installment
  from
   `datamart-finance.staging.v_order__cart`
  where
    payment_status = 'paid'
    and payment_timestamp >= (select filter2 from fd)
    and payment_timestamp < (select filter4 from fd)
  group by
    order_id
)
, ocr as (
  select
    distinct
    order_id
    , 'b2b_cermati' as ocr_payment_source
  from
   `datamart-finance.staging.v_order__cart`
  where
    payment_status = 'paid'
    and reseller_type = 'reseller'
    and reseller_id in (34313834,33918862) /* PT Dwi Cermat Indonesia / BCA - Cermati */
    and payment_timestamp >= (select filter2 from fd)
    and payment_timestamp < (select filter4 from fd)
  group by
    order_id
)
, ocd as (
  select
    order_id
    , 1 as is_top_up_affiliate_flag
  from
   `datamart-finance.staging.v_order__cart_detail`
  where
    created_timestamp >= (select filter3 from fd)
    and created_timestamp < (select filter4 from fd)
    and order_type = 'affiliate_top_up'
  group by
    1
)
, op as (
  select
    order_id
    , max(payment_amount) as payment_amount
    , case
        when string_agg(distinct ocr_payment_source) is not null then string_agg(distinct ocr_payment_source)
        else string_agg(distinct payment_source)
      end as payment_source
  from 
   `datamart-finance.staging.v_order__payment`
    left join ocr using(order_id)
  where
    payment_id = 1
    -- and payment_flag = 1
    -- and payment_timestamp >= (select filter3 from fd)
    -- and payment_timestamp < (select filter4 from fd) /*@wahyu 20220913 update because transactions using the web are still error payment_timestamp is null*/
  group by
    order_id
)
, tppt as (
    select 
      distinct
      reference_id as order_id
      , lower(string_agg(distinct paymentGateway)) as payment_gateway
    FROM `datamart-finance.staging.v_tix_payment__payment_transaction` 
    where 
      createdDate >= (select filter3 from fd)
      and createdDate < (select filter4 from fd)
    group by 1
)
, occ as (
  select
    order_id
    , string_agg(distinct auth_code) as auth_code
    , case
        when string_agg(distinct pg_name) = '' then string_agg(distinct payment_gateway)
        else string_agg(distinct pg_name)
      end as payment_gateway
    , string_agg(distinct acquiring_bank) as acquiring_bank
  from
   `datamart-finance.staging.v_order__credit_card` occ
   left join tppt using (order_id)
  where
    payment_timestamp >= (select filter3 from fd)
    and payment_timestamp < (select filter4 from fd)
  group by
    order_id
)
, btr as (
  select
    distinct
    safe_cast(order_id as int64) as order_id
    , bank_id
    , id as payment_id
    , timestamp(datetime(max(payment_date), 'Asia/Jakarta')) as payment_timestamp
    , max(credit) as total_credit
  from
   `datamart-finance.staging.v_bank__transfer` 
  where timestamp_insert >= (select filter1 from fd)
  and timestamp_insert < (select filter4 from fd)
  and payment_date >= '2020-01-06 17:00:00' /* only need data after 7 jan 2020*/
  and debit = 0
  group by 1,2,3
)
, fpm as (
  select 
    payment_source
    , reference_id_workday as reference_id_bank
    , payment_type_workday as payment_type_bank
    /* , external_reference */
  from
   `datamart-finance.staging.v_workday_mapping_payment_bank`
  where
    pg_name is null
    and bank_id is null
  group by
    1,2,3
)
, fpm2 as (
  select 
    payment_source
    , pg_name as payment_gateway
    , acquiring_bank
    , reference_id_workday as reference_id_bank
    , payment_type_workday as payment_type_bank
    /* , external_reference */
  from 
   `datamart-finance.staging.v_workday_mapping_payment_bank`
  where
    pg_name is not null
  group by
    1,2,3,4,5
)
, fpm3 as (
  select 
    payment_source
    , reference_id_workday as reference_id_bank
    , payment_type_workday as payment_type_bank
    /* , external_reference */
    , safe_cast(bank_id as int64) as bank_id
  from
   `datamart-finance.staging.v_workday_mapping_payment_bank`
  where
    pg_name is null
    and bank_id is not null
  group by
    1,2,3,4
)
, wmpc as (
  select
    *
  from
   `datamart-finance.staging.v_workday_mapping_payment_charge` 
)
, fact as (
  select
    'GTN_IDN' as company
    , order_id
    , payment_id
    , auth_code
    , round(payment_amount,2) as payment_amount
    , 'IDR' as payment_currency 
    , payment_timestamp
    , transaction_timestamp
    , replace(coalesce(fpm3.reference_id_bank, fpm2.reference_id_bank, fpm.reference_id_bank),' ','_') as bank_account
    , payment_gateway
    , payment_source
    , coalesce(wmpc.nominal_value + round(safe_divide(wmpc.percentage_value*op.payment_amount,100),2),0) as pg_charge
    , replace(coalesce(fpm3.payment_type_bank, fpm2.payment_type_bank, fpm.payment_type_bank),' ','_') as payment_type_bank
    /*, btr.total_credit - case when wmpc.type = 'nett' then coalesce(wmpc.nominal_value + ceil(wmpc.percentage_value*op.payment_amount/100),0) else 0 end as bank_deposit_amount */ /* cancelled based on jira DBI - 1407*/
    , round(safe_cast(btr.total_credit as float64),2) as bank_deposit_amount
    /*, coalesce(fpm3.external_reference, fpm2.external_reference, fpm.external_reference) as external_reference */
    , safe_cast(order_id as string) as external_reference
    , coalesce(is_top_up_affiliate_flag,0) as is_top_up_affiliate_flag
  from
    oc
    left join ocd using (order_id)
    left join op using (order_id)
    left join occ using (order_id)
    inner join btr using (order_id)
    left join fpm using (payment_source)
    left join fpm2 using (payment_gateway,acquiring_bank,payment_source)
    left join fpm3 using (payment_source,bank_id)
    left join wmpc on wmpc.payment_type_bank = replace(coalesce(fpm3.payment_type_bank, fpm2.payment_type_bank,fpm.payment_type_bank),' ','_') and wmpc.installment = oc.cc_installment and date(transaction_timestamp) between wmpc.start_date and coalesce(wmpc.end_date,current_date())
)
, tr as (
  select 
    * except (rn)
  from
    (
      select
        *
        , row_number() over(partition by order_id, payment_id order by processed_timestamp desc) as rn
      from
        `datamart-finance.datasource_workday.customer_payment_raw`
      where date(payment_timestamp) >= (select date(filter2,'Asia/Jakarta') from fd)
    )
  where rn = 1
)
, upsert_flag as (
  select
    fact.*
    , case 
        when tr.order_id is null then 'insert'
        else 'update'
      end as upsert_status
  from
    fact
    left join tr on
     tr.order_id = fact.order_id
     and tr.payment_id = fact.payment_id
)
, ins as (
  select
    upsert_flag.*
    , timestamp(datetime(current_timestamp(),'Asia/Jakarta')) as processed_timestamp
  from
    upsert_flag
  where upsert_status = 'insert'
)
, upd as (
  select
    fact.*
    , timestamp(datetime(current_timestamp(),'Asia/Jakarta')) as processed_timestamp
  from
    (select * from upsert_flag where upsert_status = 'update') fact
  left join tr on
    (tr.company = fact.company)
    and (tr.order_id = fact.order_id)
    and (tr.payment_id = fact.payment_id or (tr.payment_id is null and fact.payment_id is null))
    and (tr.auth_code = fact.auth_code or (tr.auth_code is null and fact.auth_code is null))
    and (tr.payment_amount = fact.payment_amount or (tr.payment_amount is null and fact.payment_amount is null))
    and (tr.payment_currency = fact.payment_currency or (tr.payment_currency is null and fact.payment_currency is null))
    and (tr.payment_timestamp = fact.payment_timestamp)
    and (tr.transaction_timestamp = fact.transaction_timestamp)
    and (tr.bank_account = fact.bank_account or (tr.bank_account is null and fact.bank_account is null))
    and (tr.payment_gateway = fact.payment_gateway or (tr.payment_gateway is null and fact.payment_gateway is null))
    and (tr.payment_source = fact.payment_source or (tr.payment_source is null and fact.payment_source is null))
    and (tr.pg_charge = fact.pg_charge or (tr.pg_charge is null and fact.pg_charge is null))
    and (tr.payment_type_bank = fact.payment_type_bank or (tr.payment_type_bank is null and fact.payment_type_bank is null))
    and (tr.bank_deposit_amount = fact.bank_deposit_amount or (tr.bank_deposit_amount is null and fact.bank_deposit_amount is null))
    and (tr.external_reference = fact.external_reference or (tr.external_reference is null and fact.external_reference is null))
    and (tr.is_top_up_affiliate_flag = fact.is_top_up_affiliate_flag)
  where
    tr.order_id is null
)
select * except(upsert_status) from ins
union all
select * except(upsert_status) from upd