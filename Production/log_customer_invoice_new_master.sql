with
lsw as (
  select 
    order_id
    , order_detail_id
    , 1 as is_sent_flag
  from
    `datamart-finance.datasource_workday.log_sent_to_workday`
  where
    calculation_type_name = 'customer_invoice'
    and date(created_timestamp) >= date_add(current_date(), interval -3 day)
  group by
    1,2
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
      where payment_date >= date_add(date(current_timestamp(), 'Asia/Jakarta'), interval -3 day)
    )
  where rn = 1
)
, fact as (
  select
    distinct
    order_id
    , order_detail_id
  from tr raw
  left join lsw using (order_id, order_detail_id)
  where 
    all_issued_flag = 1 
    and is_sent_flag is null
    and event_data_error_flag = 0 
    and pay_at_hotel_flag = 0 
    and is_supplier_flight_not_found_flag = 0
    and 
      (
        new_supplier_flag = 1
        or new_product_provider_flag = 1
        or new_b2b_online_and_offline_flag = 1
        or new_b2b_corporate_flag = 1
      )
    and is_amount_valid_flag = 1
)

select
  order_id
  , order_detail_id
  , null as payment_id
  , 'customer_invoice' as calculation_type_name
  , 'new_master' as status_name
  , timestamp(datetime(current_timestamp(), 'Asia/Jakarta')) as created_timestamp
from
  fact