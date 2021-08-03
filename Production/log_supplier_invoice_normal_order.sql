with
lsw as (
  select 
    order_id
    , order_detail_id
    , 1 as is_ci_sent_flag
  from
    `datamart-finance.datamart_edp.log_sent_to_workday`
  where
    calculation_type_name = 'customer_invoice'
    and status_name = 'normal_order'
    and date(created_timestamp) = date(current_timestamp(),'Asia/Jakarta')
  group by
    1,2
)
, lsw2 as (
  select 
    distinct
    order_id
    , order_detail_id
    , 1 as is_supplier_invoice_sent_flag
  from
    `datamart-finance.datamart_edp.log_sent_to_workday`
  where
    calculation_type_name = 'supplier_invoice'
    and date(created_timestamp) >= date_add(date(current_timestamp(),'Asia/Jakarta'), interval -3 day)
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
        , row_number() over(partition by order_id, order_detail_id, spend_category order by processed_timestamp desc) as rn
      from
        `datamart-finance.datamart_edp.supplier_invoice_raw_OTW`
      where date(invoice_date) >= date_add(date(current_timestamp(),'Asia/Jakarta'), interval -3 day)
    )
  where rn = 1
)
select
  distinct
    order_id
    , order_detail_id
    , null as payment_id
    , 'supplier_invoice' as calculation_type_name
    , 'normal_order' as status_name
  , timestamp(datetime(current_timestamp(), 'Asia/Jakarta')) as created_timestamp
  from
    tr
    left join lsw using (order_id, order_detail_id)
    left join lsw2 using (order_id, order_detail_id)
  where 
    (is_supplier_invoice_sent_flag is null and product_provider = 'Tiketpoint_redemeed')
    or (is_supplier_invoice_sent_flag is null and is_ci_sent_flag is not null)