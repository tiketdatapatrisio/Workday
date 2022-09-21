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
, tr_intercompany as (
  select
    concat(order_id,'_RDF_',json_extract_scalar(ij,'$.pair_company')) as order_id
    , order_detail_id
    , concat('GTN_',json_extract_scalar(ij,'$.pair_company')) as company
    , concat('GTN_',json_extract_scalar(ij,'$.company')) as customer_id
    , 'Intercompany' as customer_type
    , selling_currency
    , refund_request_date as payment_timestamp
    , product_category
    , product_provider
    , supplier
    , payment_source
    , payment_gateway
    , 'PG_Affiliate_Deposit' as payment_type_bank
    , hotel_checkoutdate
    , authentication_code
    , virtual_account
    , giftcard_voucher
    , promocode_name
    , booking_code
    , ticket_number
    , memo_flight
    , sales_pax as quantity
    , 'Refund_Hotel' as revenue_category
    , safe_divide(si_amount,sales_pax) as selling_price
    , cogs as extended_amount
    , memo_hotel as memo
    , json_extract_scalar(ij,'$.pair_company') as pair_company /*EDP, 21 Feb 2022 need pair company column for CIA Intercompany*/
  from
    tr
  left join
    unnest (json_extract_array(intercompany_json)) as ij
  left join
    lsw using (order_id, order_detail_id)
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
, intercompany as (
  select
    coalesce(safe_cast(order_id as string),'') as order_id
    , coalesce(safe_cast(customer_id as string),'') as company
    , coalesce(safe_cast(company as string),'') as customer_id
    , coalesce(safe_cast(customer_type as string),'') as customer_type
    , coalesce(safe_cast(selling_currency as string),'') as selling_currency
    , coalesce(safe_cast(payment_timestamp as string),'') as payment_timestamp
    , coalesce(safe_cast(revenue_category as string),'') as revenue_category
    , coalesce(safe_cast(product_category as string),'') as product_category
    , coalesce(safe_cast(round(selling_price,2) as string),'0') as selling_price
    , coalesce(safe_cast(product_provider as string),'') as product_provider
    , coalesce(safe_cast(supplier as string),'') as supplier
    , coalesce(safe_cast(quantity as string),'0') as quantity
    , coalesce(safe_cast(round(extended_amount,2) as string),'0') as extended_amount
    , coalesce(concat("'",safe_cast(authentication_code as string)),'') as authentication_code
    , coalesce(safe_cast(virtual_account as string),'') as virtual_account
    , coalesce(safe_cast(giftcard_voucher as string),'') as giftcard_voucher
    , coalesce(safe_cast(promocode_name as string),'') as promocode_name
    , coalesce(concat("'",safe_cast(booking_code as string)),'') as booking_code
    , coalesce(concat("'",safe_cast(ticket_number as string)),'') as ticket_number
    , coalesce(safe_cast(payment_source as string),'') as payment_source
    , coalesce(safe_cast(payment_gateway as string),'') as payment_gateway
    , coalesce(safe_cast(payment_type_bank as string),'') as payment_type_bank
    , coalesce(safe_cast(memo as string),'') as memo
    , 'Deposit_Interco' as deposit_rev_category
    , '' as intercompany
    , coalesce(safe_cast(case
        when customer_type = 'Intercompany' then '' /*EDP, 21 Feb 2022 Request from Acc due date leave blank*/
        when customer_id in ('34272813', '32545767','34382690','34423384','34433582','34432620','34451081') then safe_cast(hotel_checkoutdate as string)        
        else '' end as string),'') as due_date_override /* Customer Invoice Adjustment Integration (add 3 column: deposit_rev_category, intercompany, due_date_override) applies to data on 12 Nov 2020 ~EDP */
  from
    tr_intercompany
)
select * from intercompany
order by payment_timestamp, order_id