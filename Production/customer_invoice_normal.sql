
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
    and date(created_timestamp) >= date_add(current_date('Asia/Jakarta'), interval -3 day)
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
      where payment_date >= date_add(date(current_timestamp(), 'Asia/Jakarta'), interval -4 day)
    )
  where rn = 1
)
, info as (
  select
    order_id
    , order_detail_id
    , company
    , customer_id
    , customer_type
    , selling_currency
    , payment_timestamp
    , product_category
    , payment_source
    , payment_gateway
    , payment_type_bank
    , [
      struct(
        revenue_category as revenue_category
        , case
            when is_flexi_reschedule and flexi_fare_diff > 0 then flexi_fare_diff
          else cogs 
          end as extended_amount
        , quantity as quantity
        , case
            when is_flexi_reschedule and flexi_fare_diff > 0 then safe_divide(flexi_fare_diff,quantity)
          else safe_divide(cogs,quantity) 
          end as selling_price
        , authentication_code as authentication_code
        , virtual_account as virtual_account
        , giftcard_voucher as giftcard_voucher
        , promocode_name as promocode_name
        , booking_code as booking_code
        , ticket_number as ticket_number
        , product_provider
        , supplier
        , case
            when revenue_category = 'Room' then memo_hotel
            when revenue_category = 'Ticket' and product_category = 'Flight' then memo_flight
          else memo_product
          end as memo
        , 1 as valid_struct_flag
        , 1 as order_for_workday
      )
      /* 6 Sept 2020: remove this struct because insurance change to multi insurance */
      /*, struct(
        'Insurance' as revenue_category
        , insurance_value as extended_amount
        , quantity as quantity
        , safe_divide(insurance_value,quantity) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , 'Cermati_Insurance' as product_provider
        , 'VR-00015460' as supplier
        , memo_insurance as memo
        , case
            when insurance_value > 0 then 1
            else 0
          end as valid_struct_flag
        , 2 as order_for_workday
      )*/
      , struct(
        'Insurance' as revenue_category
        , cancel_insurance_value as extended_amount
        , quantity as quantity
        , safe_divide(cancel_insurance_value,quantity) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , 'Cermati_Antigalau' as product_provider
        , 'VR-00015460' as supplier
        , memo_cancel_insurance as memo
        , case
            when cancel_insurance_value > 0 then 1
            else 0
          end as valid_struct_flag
        , 3 as order_for_workday
      )
      , struct(
        case 
          when revenue_category = 'Hotel_Voucher' then 'Comm_Hotel_Voucher'
          else 'Commision'
        end as revenue_category
        , commission as extended_amount
        , quantity as quantity
        , safe_divide(commission,quantity) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , memo_product as memo
        , case
            when is_flexi_reschedule and flexi_fare_diff > 0 then 0
            when commission <> 0 then 1
            else 0
          end as valid_struct_flag
        , 4 as order_for_workday
      )
      , struct(
        'Subsidy' as revenue_category
        , subsidy as extended_amount
        , quantity as quantity
        , safe_divide(subsidy,quantity) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , memo_product as memo
        , case
            when subsidy < 0 then 1
            else 0
          end as valid_struct_flag
        , 5 as order_for_workday
      )
      , struct(
        'Vat_Out' as revenue_category
        , vat_out as extended_amount
        , quantity as quantity
        , safe_divide(vat_out,quantity) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , memo_product as memo
        , case
            when vat_out > 0 then 1
            else 0
          end as valid_struct_flag
        , 6 as order_for_workday
      )
      , struct(
        'Upselling' as revenue_category
        , upselling as extended_amount
        , quantity as quantity
        , safe_divide(upselling,quantity) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , memo_product as memo
        , case
            when upselling > 0 then 1
            else 0
          end as valid_struct_flag
        , 7 as order_for_workday
      )
      , struct(
        'Bagage' as revenue_category
        , baggage_fee as extended_amount
        , 1 as quantity
        , safe_divide(baggage_fee,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , memo_flight as memo
        , case
            when baggage_fee > 0 then 1
            else 0
          end as valid_struct_flag
        , 8 as order_for_workday
      )
      , struct(
        'Promocode' as revenue_category
        , promocode_value as extended_amount
        , 1 as quantity
        , safe_divide(promocode_value,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , concat(safe_cast(order_id as string),' - ', promocode_name) as memo
        , case
            when promocode_value < 0 then 1
            else 0
          end as valid_struct_flag
        , 9 as order_for_workday
      )
      , struct(
        'GV_Discount' as revenue_category
        , giftvoucher_value as extended_amount
        , 1 as quantity
        , safe_divide(giftvoucher_value,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , memo_giftvoucher as memo
        , case
            when giftvoucher_value < 0 then 1
            else 0
          end as valid_struct_flag
        , 10 as order_for_workday
      )
      , struct(
        'Refund_Reschedule' as revenue_category
        , refund_deposit_value-reschedule_fee_flight-reschedule_miscellaneous_amount-reschedule_promocode_amount as extended_amount
        , 1 as quantity
        , safe_divide(refund_deposit_value-reschedule_fee_flight-reschedule_miscellaneous_amount+reschedule_promocode_amount,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider_reschedule_flight as product_provider
        , supplier_reschedule_flight as supplier
        , concat(safe_cast(order_id as string),' - ', refund_deposit_name) as memo
        , case
            when is_flexi_reschedule and flexi_fare_diff > 0 then 0
            when refund_deposit_value < 0 then 1
            else 0
          end as valid_struct_flag
        , 11 as order_for_workday
      )
      , struct(
        'Reschedule_fee' as revenue_category
        , flexi_reschedule_fee as extended_amount
        , 1 as quantity
        , flexi_reschedule_fee as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider_reschedule_flight as product_provider
        , supplier_reschedule_flight as supplier
        , concat(safe_cast(order_id as string),' - ', refund_deposit_name) as memo
        , case
            when is_flexi_reschedule and flexi_fare_diff > 0 and flexi_reschedule_fee > 0 then 1
            else 0
          end as valid_struct_flag
        , 12 as order_for_workday
      )
      , struct(
        'Reschedule_Fee' as revenue_category
        , reschedule_fee_flight as extended_amount
        , 1 as quantity
        , safe_divide(reschedule_fee_flight,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , concat(safe_cast(order_id as string),' - ', refund_deposit_name) as memo
        , case
            when reschedule_fee_flight > 0 then 1
            else 0
          end as valid_struct_flag
        , 13 as order_for_workday
      )
      , struct(
        'Refund_Payable' as revenue_category
        , reschedule_cashback_amount as extended_amount
        , 1 as quantity
        , reschedule_cashback_amount as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , concat(safe_cast(order_id as string),' - ', refund_deposit_name) as memo
        , case
            when reschedule_cashback_amount > 0 then 1
            else 0
          end as valid_struct_flag
        , 14 as order_for_workday
      )
      , struct(
        'Miscellaneous_Expense' as revenue_category
        , reschedule_miscellaneous_amount as extended_amount
        , 1 as quantity
        , safe_divide(reschedule_miscellaneous_amount,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , concat(safe_cast(order_id as string),' - ', refund_deposit_name) as memo
        , case
            when reschedule_miscellaneous_amount > 0 then 1
            else 0
          end as valid_struct_flag
        , 15 as order_for_workday
      )
      , struct(
        'Promocode' as revenue_category
        , reschedule_promocode_amount as extended_amount
        , 1 as quantity
        , safe_divide(reschedule_promocode_amount,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , concat(safe_cast(order_id as string),' - ', refund_deposit_name) as memo
        , case
            when reschedule_promocode_amount <> 0 then 1
            else 0
          end as valid_struct_flag
        , 16 as order_for_workday
      )
      , struct(
        'Rebooking_Sales' as revenue_category
        , rebooking_sales_hotel as extended_amount
        , 1 as quantity
        , safe_divide(rebooking_sales_hotel,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , memo_hotel as memo
        , case
            when rebooking_sales_hotel != 0 then 1
            else 0
          end as valid_struct_flag
        , 17 as order_for_workday
      )
      , struct(
        'TixPoint_Disc' as revenue_category
        , tiketpoint_value as extended_amount
        , 1 as quantity
        , safe_divide(tiketpoint_value,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , memo_product as memo
        , case
            when tiketpoint_value < 0 then 1
            else 0
          end as valid_struct_flag
        , 18 as order_for_workday
      )
      , struct(
        'Bank_Charges' as revenue_category
        , payment_charge+pg_charge as extended_amount
        , 1 as quantity
        , safe_divide(payment_charge+pg_charge,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , concat(safe_cast(order_id as string),' - Absorbed Bank Charge : ', payment_source) as memo
        , case
            when payment_charge <= 0 and payment_charge+pg_charge != 0 then 1
            else 0
          end as valid_struct_flag
        , 19 as order_for_workday
      )
      , struct(
        'Bank_Charges' as revenue_category
        , payment_charge as extended_amount
        , 1 as quantity
        , safe_divide(payment_charge,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , concat(safe_cast(order_id as string),' - Absorbed Bank Charge : ', payment_source) as memo
        , case
            when payment_charge > 0 and cc_installment = 0 then 1
            else 0
          end as valid_struct_flag
        , 20 as order_for_workday
      )
      , struct(
        'Installment' as revenue_category
        , payment_charge as extended_amount
        , 1 as quantity
        , safe_divide(payment_charge,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , concat(safe_cast(order_id as string),' - Installment Charges : ', acquiring_bank ,' tenor ', safe_cast(cc_installment as string), ' months') as memo
        , case
            when cc_installment > 0 and payment_charge > 0 then 1
            else 0
          end as valid_struct_flag
        , 21 as order_for_workday
      )
      , struct(
        'Gateway_Charges' as revenue_category
        , pg_charge * -1 as extended_amount
        , 1 as quantity
        , safe_divide(pg_charge * -1,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , concat(safe_cast(order_id as string),' - Absorbed Bank Charge : ', payment_source) as memo
        , case
            when pg_charge > 0 and cc_installment >= 0 and payment_charge <= 0 then 1
            else 0
          end as valid_struct_flag
        , 22 as order_for_workday
      )
      , struct(
        'Rapid_Test' as revenue_category
        , halodoc_sell_price_amount as extended_amount
        , halodoc_pax_count as quantity
        , safe_divide(halodoc_sell_price_amount,halodoc_pax_count) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , case
          when lower(memo_halodoc) like '%siloam%' then 'Siloam'
          when lower(memo_halodoc) like '%halodoc%' then 'Halodoc'
          when lower(memo_halodoc) like '%farma%' then 'Kimia_Farma'
          else '' end as product_provider
        , case
          when lower(memo_halodoc) like '%siloam%' then '34276792'
          when lower(memo_halodoc) like '%halodoc%' then 'VR-00015459'
          when lower(memo_halodoc) like '%farma%' then '34305569'
          else '' end as supplier
        , memo_halodoc as memo
        , case
            when is_has_halodoc_flag > 0 and halodoc_sell_price_amount > 0 then 1
            else 0
          end as valid_struct_flag
        , 23 as order_for_workday
      )
      , struct(
        'Convenience_Fee' as revenue_category
        , convenience_fee_amount as extended_amount
        , 1 as quantity
        , safe_divide(convenience_fee_amount,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , memo_convenience_fee as memo
        , case
            when convenience_fee_amount > 0  then 1
            else 0
          end as valid_struct_flag
        , 24 as order_for_workday
      )
      , struct(
        'Rebooking_Sales' as revenue_category
        , safe_divide(diff_amount_rebooking,1) as extended_amount
        , 1 as quantity
        , safe_divide(diff_amount_rebooking,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , concat(safe_cast(order_id as string),' - Rebook from orderId #', old_id_rebooking) as memo
        , case
            when is_rebooking_flag > 0 and diff_amount_rebooking < 0  then 1
            else 0
          end as valid_struct_flag
        , 25 as order_for_workday
      )
      , struct(
        'Refund_Payable' as revenue_category
        , safe_divide(diff_amount_rebooking,1) as extended_amount
        , 1 as quantity
        , safe_divide(diff_amount_rebooking,1) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , product_provider
        , supplier
        , concat(safe_cast(order_id as string),' - Rebook from orderId #', old_id_rebooking) as memo
        , case
            when is_rebooking_flag > 0 and diff_amount_rebooking > 0 then 1
            else 0
          end as valid_struct_flag
        , 26 as order_for_workday
      )
    ] as info_array
  from tr
  left join lsw using (order_id, order_detail_id)
  where 
    all_issued_flag = 1 
    and is_sent_flag is null
    and event_data_error_flag = 0 
    and pay_at_hotel_flag = 0 
    and (
          not is_flexi_reschedule
          or (
               is_flexi_reschedule
               and flexi_fare_diff>0
             )
        )
    and new_supplier_flag = 0
    and new_product_provider_flag = 0
    and new_b2b_online_and_offline_flag = 0
    and new_b2b_corporate_flag = 0
    and is_supplier_flight_not_found_flag = 0
    and is_amount_valid_flag = 1
)
, vertical as (
select
    order_id as order_id
    , company as company
    , customer_id as customer_id
    , customer_type as customer_type
    , selling_currency as selling_currency
    , payment_timestamp as payment_timestamp
    , info_array.revenue_category as revenue_category
    , case 
        when revenue_category = 'Insurance' then 'Insurance' 
        when revenue_category = 'Rapid_Test' then 'Others' 
        else product_category end as product_category
    , coalesce(round(info_array.selling_price,2),0) as selling_price
    , product_provider as product_provider
    , supplier as supplier
    , coalesce(info_array.quantity,0) as quantity
    , coalesce(round(info_array.extended_amount,2),0) as extended_amount
    , concat("'",safe_cast(info_array.authentication_code as string)) as authentication_code
    , info_array.virtual_account as virtual_account
    , info_array.giftcard_voucher as giftcard_voucher
    , info_array.promocode_name as promocode_name
    , concat("'",safe_cast(info_array.booking_code as string)) as booking_code
    , concat("'",safe_cast(info_array.ticket_number as string))as ticket_number
    , payment_source as payment_source
    , payment_gateway as payment_gateway
    , payment_type_bank as payment_type_bank
    , info_array.memo as memo
    , info_array.order_for_workday as order_by_for_workday
from
  info
cross join
  unnest (info.info_array) as info_array
where info_array.valid_struct_flag = 1
order by payment_timestamp, order_id , info_array.order_for_workday asc
)
, tr_add_ons as (
  select
    order_id
    , order_detail_id
    , company
    , customer_id
    , customer_type
    , selling_currency
    , payment_timestamp
    , product_category
    , product_provider
    , supplier
    , payment_source
    , payment_gateway
    , payment_type_bank
    , authentication_code
    , virtual_account
    , giftcard_voucher
    , promocode_name
    , booking_code
    , ticket_number
    , memo_product
    , add_ons.add_ons_revenue_category
    , add_ons.add_ons_commission_revenue_category
    , add_ons.add_ons_hotel_quantity
    , add_ons.add_ons_hotel_net_price_amount
    , add_ons.add_ons_hotel_sell_price_amount
    , add_ons.add_ons_hotel_commission_amount
  from
    tr
  cross join
    unnest (add_ons_hotel_detail_array) as add_ons
  left join lsw using (order_id, order_detail_id)
  where 
    all_issued_flag = 1 
    and tr.add_ons_hotel_detail_array is not null
    and is_sent_flag is null
    and event_data_error_flag = 0 
    and pay_at_hotel_flag = 0 
    and (
          not is_flexi_reschedule
          or (
               is_flexi_reschedule
               and flexi_fare_diff>0
             )
        )
    and new_supplier_flag = 0
    and new_product_provider_flag = 0
    and new_b2b_online_and_offline_flag = 0
    and new_b2b_corporate_flag = 0
    and is_supplier_flight_not_found_flag = 0
    and is_amount_valid_flag = 1
)
, info_add_ons as (
  select
  order_id
    , order_detail_id
    , company
    , customer_id
    , customer_type
    , selling_currency
    , payment_timestamp
    , product_category
    , payment_source
    , payment_gateway
    , payment_type_bank
    , [
      struct(
        add_ons_revenue_category as revenue_category
        , add_ons_hotel_net_price_amount as extended_amount
        , add_ons_hotel_quantity as quantity
        , safe_divide(add_ons_hotel_net_price_amount,add_ons_hotel_quantity) as selling_price
        , authentication_code as authentication_code
        , virtual_account as virtual_account
        , giftcard_voucher as giftcard_voucher
        , promocode_name as promocode_name
        , booking_code as booking_code
        , ticket_number as ticket_number
        , product_provider
        , supplier
        , memo_product as memo
        , 1 as valid_struct_flag
        , 22 as order_for_workday
      )
      , struct(
        add_ons_commission_revenue_category as revenue_category
        , add_ons_hotel_commission_amount as extended_amount
        , add_ons_hotel_quantity as quantity
        , safe_divide(add_ons_hotel_commission_amount,add_ons_hotel_quantity) as selling_price
        , authentication_code as authentication_code
        , virtual_account as virtual_account
        , giftcard_voucher as giftcard_voucher
        , promocode_name as promocode_name
        , booking_code as booking_code
        , ticket_number as ticket_number
        , product_provider
        , supplier
        , memo_product as memo
        , 1 as valid_struct_flag
        , 23 as order_for_workday
      )
    ] as info_array
  from
    tr_add_ons
)
, add_ons as (
  select
    order_id as order_id
    , company as company
    , customer_id as customer_id
    , customer_type as customer_type
    , selling_currency as selling_currency
    , payment_timestamp as payment_timestamp
    , info_array.revenue_category as revenue_category
    , product_category as product_category
    , coalesce(round(info_array.selling_price,2),0) as selling_price
    , product_provider as product_provider
    , supplier as supplier
    , coalesce(info_array.quantity,0) as quantity
    , coalesce(round(info_array.extended_amount,2),0) as extended_amount
    , concat("'",safe_cast(info_array.authentication_code as string)) as authentication_code
    , info_array.virtual_account as virtual_account
    , info_array.giftcard_voucher as giftcard_voucher
    , info_array.promocode_name as promocode_name
    , concat("'",safe_cast(info_array.booking_code as string)) as booking_code
    , concat("'",safe_cast(info_array.ticket_number as string))as ticket_number
    , payment_source as payment_source
    , payment_gateway as payment_gateway
    , payment_type_bank as payment_type_bank
    , info_array.memo as memo
    , info_array.order_for_workday as order_by_for_workday
  from
    info_add_ons
  cross join
    unnest (info_add_ons.info_array) as info_array
  where info_array.valid_struct_flag = 1
)
, tr_flight_multi_insurance as (
  select
    order_id
    , order_detail_id
    , company
    , customer_id
    , customer_type
    , selling_currency
    , payment_timestamp
    , 'Insurance' as product_category
    , product_provider
    , supplier
    , payment_source
    , payment_gateway
    , payment_type_bank
    , authentication_code
    , virtual_account
    , giftcard_voucher
    , promocode_name
    , booking_code
    , ticket_number
    , memo_product
    , quantity
    , safe_cast(json_extract_scalar(fmi,'$.insurance_value') as float64) as insurance_value 
    , json_extract_scalar(fmi,'$.insurance_name') as insurance_name 
    , json_extract_scalar(fmi,'$.insurance_issue_code') as insurance_issue_code 
    , json_extract_scalar(fmi,'$.memo_insurance') as memo_insurance 
  from
    tr
  left join
    unnest (json_extract_array(flight_insurance_json)) as fmi
  left join lsw using (order_id, order_detail_id)
  where 
    all_issued_flag = 1 
    and tr.flight_insurance_json is not null
    and is_sent_flag is null
    and event_data_error_flag = 0 
    and pay_at_hotel_flag = 0 
    and (
          not is_flexi_reschedule
          or (
               is_flexi_reschedule
               and flexi_fare_diff>0
             )
        )
    and new_supplier_flag = 0
    and new_product_provider_flag = 0
    and new_b2b_online_and_offline_flag = 0
    and new_b2b_corporate_flag = 0
    and is_supplier_flight_not_found_flag = 0
    and is_amount_valid_flag = 1
)
, info_flight_multi_insurance as (
  select
  order_id
    , order_detail_id
    , company
    , customer_id
    , customer_type
    , selling_currency
    , payment_timestamp
    , product_category
    , payment_source
    , payment_gateway
    , payment_type_bank
    , [
      struct(
        'Insurance' as revenue_category
        , insurance_value as extended_amount
        , quantity as quantity
        , safe_divide(insurance_value,quantity) as selling_price
        , null as authentication_code
        , null as virtual_account
        , null as giftcard_voucher
        , null as promocode_name
        , null as booking_code
        , null as ticket_number
        , 'Cermati_Insurance' as product_provider
        , 'VR-00015460' as supplier
        , memo_insurance as memo
        , 1 as valid_struct_flag
        , 2 as order_for_workday
      )
    ] as info_array
  from
    tr_flight_multi_insurance
)
, flight_multi_insurance as (
  select
    order_id as order_id
    , company as company
    , customer_id as customer_id
    , customer_type as customer_type
    , selling_currency as selling_currency
    , payment_timestamp as payment_timestamp
    , info_array.revenue_category as revenue_category
    , product_category as product_category
    , coalesce(round(info_array.selling_price,2),0) as selling_price
    , product_provider as product_provider
    , supplier as supplier
    , coalesce(info_array.quantity,0) as quantity
    , coalesce(round(info_array.extended_amount,2),0) as extended_amount
    , concat("'",safe_cast(info_array.authentication_code as string)) as authentication_code
    , safe_cast(info_array.virtual_account as string) as virtual_account
    , safe_cast(info_array.giftcard_voucher as string) as giftcard_voucher
    , safe_cast(info_array.promocode_name as string) as promocode_name
    , concat("'",safe_cast(info_array.booking_code as string)) as booking_code
    , concat("'",safe_cast(info_array.ticket_number as string))as ticket_number
    , payment_source as payment_source
    , payment_gateway as payment_gateway
    , payment_type_bank as payment_type_bank
    , info_array.memo as memo
    , info_array.order_for_workday as order_by_for_workday
  from
    info_flight_multi_insurance
  cross join
    unnest (info_flight_multi_insurance.info_array) as info_array
  where info_array.valid_struct_flag = 1
)
select * except(order_by_for_workday) from (
select * from vertical
union all
select * from add_ons
union all
select * from flight_multi_insurance)
order by payment_timestamp, order_id , order_by_for_workday asc