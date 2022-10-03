with
lsw as (
  select
    distinct order_id
    , order_detail_id
    , true as is_sent_flag
  from
    `datamart-finance.datasource_workday.log_sent_to_workday`
  where
    calculation_type_name = 'supplier_invoice_refund'
  group by
    1,2
)
, tr as (
  select
    * 
  from
    `datamart-finance.datasource_workday.refund_raw`
  where
    date(refund_request_date) between
      date_add(date(current_timestamp(),'Asia/Jakarta'), interval -8 day)
      and date_add(date(current_timestamp(),'Asia/Jakarta'), interval -1 day)
  qualify row_number() over(partition by order_id, order_detail_id, refund_id order by processed_timestamp desc) = 1
)
, refund as ( /*insert refund_id instead of order_detail_id*/
  select
  distinct
    tr.order_id
    , refund_id as order_detail_id
    , null as payment_id
    , 'supplier_invoice_refund' as calculation_type_name
    , 'normal_order' as status_name
    , timestamp(datetime(current_timestamp(), 'Asia/Jakarta')) as created_timestamp
  from
    tr
  left join
    lsw on tr.order_id = lsw.order_id and tr.refund_id = lsw.order_detail_id
  where 
    is_refund_valid_flag
    and is_sent_flag is null
)
select * from refund