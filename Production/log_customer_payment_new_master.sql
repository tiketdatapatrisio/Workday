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
    and status_name = 'new_master'
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
  distinct
  order_id
  , null as order_detail_id
  , payment_id
  , 'customer_payment' as calculation_type_name
  , 'new_master' as status_name
  , timestamp(datetime(current_timestamp(), 'Asia/Jakarta')) as created_timestamp
from
  tr
  left join lsw using (order_id, payment_id)
  left join lsw2 using (order_id)
where
  (is_customer_payment_sent_flag is null and is_customer_invoice_sent_flag = 1)