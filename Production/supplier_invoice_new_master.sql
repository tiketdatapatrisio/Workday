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
        * except(booking_code)
        , coalesce
          (
            concat(split(booking_code,' - ')[safe_offset(0)],' - ',regexp_replace(trim(tier_code), '^.|.$', ''))
            , booking_code
          ) as booking_code
        , row_number() over(partition by order_id, order_detail_id, spend_category,tier_code order by processed_timestamp desc) as rn
      from
        `datamart-finance.datasource_workday.supplier_invoice_raw`
        left join unnest (json_extract_array(array_reverse(split(booking_code,' - '))[safe_offset(0)])) as tier_code
      where date(invoice_date) >= date_add(date(current_timestamp(),'Asia/Jakarta'), interval -3 day)
      and total_line_amount is not null /*to handle order with zero payment, 15 Mei 2021 - EDP */
    )
  where rn = 1
)
select
    coalesce(Concat('"',Company,'"'), '""') as Company
    , coalesce(Concat('"',invoice_currency,'"'), '""') as invoice_currency
    , coalesce(Concat('"',supplier_reference_id,'"'), '""') as supplier_reference_id
    , coalesce(Concat('"',safe_cast(invoice_date as string),'"'), '""') as invoice_date
    , coalesce(Concat('"',safe_cast(order_id as string),'"'), '""') as order_id
    , coalesce(Concat('"',safe_cast(order_detail_id as string),'"'), '""') as order_detail_id
    , coalesce(Concat('"',safe_cast(due_date as string),'"'), '""') as due_date
    , coalesce(Concat('"',order_detail_name,'"'), '""') as order_detail_name
    , coalesce(Concat('"',spend_category,'"'), '""') as spend_category
    , coalesce(Concat('"',safe_cast(coalesce(quantity,1) as string),'"'), '""') as quantity
    /* , coalesce(Concat('"',safe_cast(round(total_line_amount,2) as string),'"'), '""') as total_line_amount -- 08 April 2021, for currency JPY & VND total line amount must be rounding, EDP*/
    , coalesce(Concat('"',
        case
          when invoice_currency in ('JPY', 'VND') then safe_cast(round(total_line_amount,0) as string)
          else safe_cast(round(total_line_amount,2) as string)
          end
          ,'"'), '""') as total_line_amount
    , coalesce(Concat('"',
        case
          when Company <> 'GTN_IDN' then ''
          else safe_cast(round(currency_conversion,2) as string)
        end
      ,'"'), '""') as currency_conversion
    , coalesce(Concat('"',booking_code,'"'), '""') as booking_code
    , coalesce(Concat('"',product_category,'"'), '""') as product_category
    , coalesce(Concat('"',product_provider,'"'), '""') as product_provider
    , coalesce(Concat('"',deposit_flag,'"'), '""') as deposit_flag
    , coalesce(Concat('"',event_name,'"'), '""') as event_name
    , '""' as payment_handling
    , coalesce(concat('"',on_hold_status,'"'),'""') as on_hold_status
    , coalesce(concat('"',memo,'"'),'""') as memo
    , coalesce(concat('"',customer_reference_id,'"'),'""') as customer_reference_id
  from
    tr
    left join lsw using (order_id, order_detail_id)
    left join lsw2 using (order_id, order_detail_id)
  where 
    is_supplier_invoice_sent_flag is null and is_ci_sent_flag is not null
  order by invoice_date asc, order_id, order_detail_id, case when spend_category like '%Bagage%' then 2 when spend_category like '%Spend_%' then 3 when spend_category in ('Add_On_Zone','Add_On_Special') then 2 else 1 end asc