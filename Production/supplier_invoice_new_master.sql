with
lsw as (
  select 
    order_id
    , order_detail_id
    , 1 as is_ci_sent_flag
  from
    `datamart-finance.datasource_workday.log_sent_to_workday`
  where
    calculation_type_name = 'customer_invoice'
    and status_name = 'new_master'
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
    `datamart-finance.datasource_workday.log_sent_to_workday`
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
        `datamart-finance.datasource_workday.supplier_invoice_raw`
      where date(invoice_date) >= date_add(date(current_timestamp(),'Asia/Jakarta'), interval -3 day)
    )
  where rn = 1
)
select
    Company as Company
    , invoice_currency as invoice_currency
    , supplier_reference_id as supplier_reference_id
    , invoice_date as invoice_date
    , order_id as order_id
    , order_detail_id as order_detail_id
    , due_date as due_date
    , order_detail_name as order_detail_name
    , spend_category as spend_category
    , coalesce(quantity,1) as quantity
    , round(total_line_amount,2) as total_line_amount
    , round(currency_conversion,2) as currency_conversion
    , booking_code as booking_code
    , product_category as product_category
    , product_provider as product_provider
    , deposit_flag as deposit_flag
    , event_name as event_name
    , null as payment_handling
    , on_hold_status as on_hold_status
    , memo as memo
    , customer_reference_id
  from
    tr
    left join lsw using (order_id, order_detail_id)
    left join lsw2 using (order_id, order_detail_id)
  where 
    is_supplier_invoice_sent_flag is null and is_ci_sent_flag is not null
  order by invoice_date asc, order_id, order_detail_id, case when spend_category like '%Bagage%' then 2 when spend_category like '%Spend_%' then 3 else 1 end asc