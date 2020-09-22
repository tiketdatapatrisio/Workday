with
lsw as (
  select 
    order_id
    , payment_id
    , 1 as is_customer_payment_sent_flag
  from
    `datamart-finance.datasource_workday.log_sent_to_workday`
  where
    calculation_type_name = 'customer_payment'
    and date(created_timestamp) >= date_add(date(current_timestamp(),'Asia/Jakarta'), interval -100 day) and date(created_timestamp) < current_date('Asia/Jakarta')
  group by
    1,2
)
, lsw2 as (
  select 
    order_id
    , 1 as is_customer_invoice_sent_flag
  from
    `datamart-finance.datasource_workday.log_sent_to_workday`
  where
    calculation_type_name = 'customer_invoice'
    and status_name = 'normal_order'
    and date(created_timestamp) >= date_add(date(current_timestamp(),'Asia/Jakarta'), interval -365 day)
  group by
    1
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
      where date(payment_timestamp) >= date_add(date(current_timestamp(),'Asia/Jakarta'), interval -100 day)
    )
  where rn = 1
)
select
  coalesce(concat('"',safe_cast(company as string),'"'),'""') as company
  ,coalesce(concat('"',safe_cast(order_id as string),'"'),'""') as order_id
  ,coalesce(concat('"',safe_cast(auth_code as string),'"'),'""') as auth_code
  ,coalesce(concat('"',safe_cast(round(payment_amount,2) as string),'"'),'""') as payment_amount
  ,coalesce(concat('"',safe_cast(payment_currency as string),'"'),'""') as payment_currency
  ,coalesce(concat('"',safe_cast(datetime(payment_timestamp) as string),'"'),'""') as payment_date
  ,coalesce(concat('"',safe_cast(datetime(transaction_timestamp) as string),'"'),'""') as transaction_date
  ,coalesce(concat('"',safe_cast(bank_account as string),'"'),'""') as bank_account
  ,coalesce(concat('"',safe_cast(payment_gateway as string),'"'),'""') as payment_gateway
  ,coalesce(concat('"',safe_cast(payment_source as string),'"'),'""') as payment_source
  ,coalesce(concat('"',safe_cast(round(pg_charge,2) as string),'"'),'""') as pg_charge
  ,coalesce(concat('"',safe_cast(payment_type_bank as string),'"'),'""') as payment_type_bank
  ,coalesce(concat('"',safe_cast(round(bank_deposit_amount,2) as string),'"'),'""') as bank_deposit_amount
  ,coalesce(concat('"',safe_cast(external_reference as string),'"'),'""') as external_reference
from
  tr
  left join lsw using (order_id,payment_id)
  left join lsw2 using (order_id)
where
  (is_customer_payment_sent_flag is null and is_customer_invoice_sent_flag = 1)
  or
  (is_customer_payment_sent_flag is null and is_top_up_affiliate_flag = 1)