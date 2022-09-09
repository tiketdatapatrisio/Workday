with
lsw as (
  select
    distinct order_id
    , order_detail_id
    , true as is_sent_flag
  from
    `datamart-finance.datasource_workday.log_sent_to_workday`
  where
    calculation_type_name = 'customer_invoice_refund'
  group by
    1,2
)
, tr1 as (
  select
    order_id
    , order_detail_id
    , refund_request_date
    , sales_pax
    , si_amount
  from
    `datamart-finance.datasource_workday.refund_raw`
  where
    Company = 'GTN_SGP'
    and date(refund_request_date) between
      date_add(date(current_timestamp(),'Asia/Jakarta'), interval -4 day)
      and date_add(date(current_timestamp(),'Asia/Jakarta'), interval -1 day)
  qualify row_number() over(partition by order_id, order_detail_id order by processed_timestamp desc) = 1
)
, tr2 as (
  select
    *
  from
    `datamart-finance.datamart_edp.customer_invoice_raw_2021`
  where
    payment_source not in ('zero_payment','invoice')
    and payment_source is not null
  qualify row_number() over(partition by order_id, order_detail_id order by processed_timestamp desc) = 1
)
, tr as (
  select
    *
  from
    tr1 join tr2 using(order_id, order_detail_id)
)
, refund as (
  select
  distinct
    order_id
    , order_detail_id
    , null as payment_id
    , 'customer_invoice_refund' as calculation_type_name
    , 'normal_order' as status_name
    , timestamp(datetime(current_timestamp(), 'Asia/Jakarta')) as created_timestamp
  from
    tr
    left join lsw using (order_id, order_detail_id)
  where 
    all_issued_flag = 1
    and is_sent_flag is null
    and tr.intercompany_json is not null
    and event_data_error_flag = 0 
    and pay_at_hotel_flag = 0 
    and (
          not is_flexi_reschedule
          or (
               is_flexi_reschedule
               and flexi_fare_diff>0
             )
        )
    and is_supplier_flight_not_found_flag = 0
    and is_amount_valid_flag = 1
)
select * from refund