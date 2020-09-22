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
    and date(created_timestamp) >= date_add(date(current_timestamp(),'Asia/Jakarta'), interval -100 day)
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
  company as company
  ,order_id as order_id
  ,auth_code as auth_code
  ,round(payment_amount,2) as payment_amount
  ,payment_currency as payment_currency
  ,datetime(payment_timestamp) as payment_date
  ,datetime(transaction_timestamp) as transaction_date
  ,bank_account as bank_account
  ,payment_gateway as payment_gateway
  ,payment_source as payment_source
  ,round(pg_charge,2) as pg_charge
  ,payment_type_bank as payment_type_bank
  ,round(bank_deposit_amount,2) as bank_deposit_amount
  ,external_reference as external_reference
from
  tr
  left join lsw using (order_id,payment_id)
  left join lsw2 using (order_id)
where
  (is_customer_payment_sent_flag is null and is_customer_invoice_sent_flag = 1)
  or
  (is_customer_payment_sent_flag is null and is_top_up_affiliate_flag = 1)