with
lsw as (
  select
    distinct order_id
    , order_detail_id
    , true as is_sent_flag
  from
    `datamart-finance.sandbox_edp.log_sent_to_workday`
  where
    calculation_type_name = 'supplier_invoice_refund'
    and date(created_timestamp) >= date(current_timestamp(),'Asia/Jakarta')
  group by
    1,2
)
, tr as (
  select
    * 
  from
    `datamart-finance.sandbox_edp.refund_raw`
  where
    date(refunded_date) = date_add(date(current_timestamp(), 'Asia/Jakarta'), interval -1 day)
  qualify row_number() over(partition by order_id, order_detail_id order by processed_timestamp desc) = 1
)
, refund as ( /*insert refund_id instead of order_detail_id*/
  select
  distinct
    order_id
    , refund_id as order_detail_id
    , null as payment_id
    , 'supplier_invoice_refund' as calculation_type_name
    , 'normal_order' as status_name
    , timestamp(datetime(current_timestamp(), 'Asia/Jakarta')) as created_timestamp
  from
    tr
    left join lsw using (order_id, order_detail_id)
  where 
    is_refund_valid_flag
    and is_sent_flag is null
)
select * from refund