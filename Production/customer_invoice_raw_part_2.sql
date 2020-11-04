with
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -1 day) as filter2
    , timestamp_add(filter1, interval 3 day) as filter3
    , timestamp_add(filter1, interval -365 day) as filter4
  from
  (
    select
       timestamp_add(timestamp(date(current_timestamp(),'Asia/Jakarta')), interval -79 hour) as filter1
  )
)
, master_data_supplier as (
  select
    * except(processed_date)
  , case 
      when lead(date_add(processed_date, interval -1 day)) over(partition by Supplier_Reference_ID order by processed_date desc) is null
        then date('2000-01-01')
      else date_add(processed_date, interval -1 day)
    end as start_date
  , date_add(processed_date, interval -1 day) as active_date
  , coalesce(lag(date_add(processed_date, interval -2 day)) over(partition by Supplier_Reference_ID order by processed_date desc), date('2099-12-31')) as end_date
from
    `datamart-finance.datasource_workday.master_data_supplier`
)
, master_data_product_provider as (
  select
    * except(processed_date)
  , case 
      when lead(date_add(processed_date, interval -1 day)) over(partition by Organization_Reference_ID order by processed_date desc) is null
        then date('2000-01-01')
      else date_add(processed_date, interval -1 day)
    end as start_date
  , date_add(processed_date, interval -1 day) as active_date
  , coalesce(lag(date_add(processed_date, interval -2 day)) over(partition by Organization_Reference_ID order by processed_date desc), date('2099-12-31')) as end_date
from
    `datamart-finance.datasource_workday.master_data_product_provider`
)
/*, master_b2b_corporate as ( -- merged to master_data_customer, Nov 2020
  select
    * except(processed_date)
  , case 
      when lead(date_add(processed_date, interval -1 day)) over(partition by business_id order by processed_date desc) is null
        then date('2000-01-01')
      else date_add(processed_date, interval -1 day)
    end as start_date
  , date_add(processed_date, interval -1 day) as active_date
  , coalesce(lag(date_add(processed_date, interval -2 day)) over(partition by business_id order by processed_date desc), date('2099-12-31')) as end_date
from
    `datamart-finance.datasource_workday.master_b2b_corporate`
)
, master_b2b_online_and_offline as (
  select
    * except(processed_date)
  , case 
      when lead(date_add(processed_date, interval -1 day)) over(partition by business_id order by processed_date desc) is null
        then date('2000-01-01')
      else date_add(processed_date, interval -1 day)
    end as start_date
  , date_add(processed_date, interval -1 day) as active_date
  , coalesce(lag(date_add(processed_date, interval -2 day)) over(partition by business_id order by processed_date desc), date('2099-12-31')) as end_date
from
    `datamart-finance.datasource_workday.master_b2b_online_and_offline`
)*/
, master_data_customer as (
  select
    * except(processed_date)
  , case 
      when lead(date_add(processed_date, interval -1 day)) over(partition by Customer_Reference_ID order by processed_date desc) is null
        then date('2000-01-01')
      else date_add(processed_date, interval -1 day)
    end as start_date
  , date_add(processed_date, interval -1 day) as active_date
  , coalesce(lag(date_add(processed_date, interval -2 day)) over(partition by Customer_Reference_ID order by processed_date desc), date('2099-12-31')) as end_date
from
    `datamart-finance.datasource_workday.master_data_customer`
)
, fact as (
  select
    c.*
    , case
        when c.product_category not in ('Flight','Train') 
            and (
              (c.product_category in ('Event','Activity','Attraction') and c.event_data_error_flag != 1)  
              or (c.product_category in ('Hotel') and c.product_provider not in ('EXPEDIA','AGODA','HOTELBEDS','BOOKING.COM')) 
              or (c.product_category in ('Car') and revenue_category not in ('Shuttle'))
            ) then 
          case
            when ms.active_date >= c.payment_date or ms.active_date is null then 1
            else 0
          end
        else 0
      end as new_supplier_flag
    , case
        when c.product_category not in ('Flight','Train') 
            and (
              (c.product_category in ('Event','Activity','Attraction') and c.event_data_error_flag != 1)  
              or (c.product_category in ('Hotel') and c.product_provider not in ('EXPEDIA','AGODA','HOTELBEDS','BOOKING.COM')) 
              or (c.product_category in ('Car') and revenue_category not in ('Shuttle'))
            ) then 
          case
            when mpp.active_date >= c.payment_date or mpp.active_date is null then 1
            else 0
          end
        else 0
      end as new_product_provider_flag
    , case
        when c.customer_type in ('B2B Online','B2B Offline') then
          case
            when mco.active_date >= c.payment_date or mco.active_date is null then 1
            else 0
          end
        else 0
      end as new_b2b_online_and_offline_flag
    , case
        when c.customer_type = 'B2B Corporate' then
          case
            when mcc.active_date >= c.payment_date or mcc.active_date is null then 1
            else 0
          end
        else 0
      end as new_b2b_corporate_flag
    , case
        when c.product_category = 'Flight' and sum(case when c.supplier is null then 1 else 0 end) over(partition by order_id) > 0 then 1
        else 0
      end as is_supplier_flight_not_found_flag
    , case 
        when sum(cogs + commission + upselling + subsidy + payment_charge + promocode_value + giftvoucher_value + refund_deposit_value + tiketpoint_value + insurance_value + cancel_insurance_value + ifnull(vat_out,0) + ifnull(baggage_fee,0) + rebooking_sales_hotel + total_add_ons_hotel_sell_price_amount + halodoc_sell_price_amount + convenience_fee_amount + diff_amount_rebooking) over(partition by order_id) > 0 
        and sum(cogs + commission + upselling + subsidy + payment_charge + promocode_value + giftvoucher_value + refund_deposit_value + tiketpoint_value + insurance_value + cancel_insurance_value + ifnull(vat_out,0) + ifnull(baggage_fee,0) + rebooking_sales_hotel + total_add_ons_hotel_sell_price_amount + halodoc_sell_price_amount + convenience_fee_amount + diff_amount_rebooking) over(partition by order_id) <> payment_amount
          then 0
        else 1
      end as is_amount_valid_flag
  from
    `datamart-finance.datasource_workday.temp_customer_invoice_raw_part_1` c
    left join master_data_supplier ms on ms.Supplier_Reference_ID = c.supplier and c.payment_date >= ms.start_date and c.payment_date < ms.end_date
    left join master_data_product_provider mpp on mpp.Organization_Reference_ID = c.product_provider and c.payment_date >= mpp.start_date and c.payment_date < mpp.end_date
   -- left join master_b2b_online_and_offline mbo on safe_cast(mbo.business_id as string) = c.customer_id and c.payment_date >= mbo.start_date and c.payment_date < mbo.end_date
   -- left join master_b2b_corporate mbc on mbc.business_id = c.customer_id and c.payment_date >= mbc.start_date and c.payment_date < mbc.end_date
    left join master_data_customer mco on mco.Customer_Reference_ID = c.customer_id and mco.Customer_Category_ID in('B2B Online','B2B Offline') and c.payment_date >= mco.start_date and c.payment_date < mco.end_date 
    left join master_data_customer mcc on mcc.Customer_Reference_ID = c.customer_id and mcc.Customer_Category_ID = 'B2B_Corporate' and c.payment_date >= mcc.start_date and c.payment_date < mcc.end_date 
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
      where payment_date >= (select date(filter1,'Asia/Jakarta') from fd)
    )
  where rn = 1
)
, append as (
  select
    fact.*
    , timestamp(datetime(current_timestamp(),'Asia/Jakarta')) as processed_timestamp
  from
    fact
  left join tr on
  (tr.order_id = fact.order_id)	
  and (tr.order_detail_id = fact.order_detail_id)	
  and (tr.company = fact.company)	
  and (tr.customer_id = fact.customer_id or (tr.customer_id is null and fact.customer_id is null))	
  and (tr.customer_type = fact.customer_type)	
  and (tr.selling_currency = fact.selling_currency 	)
  and (tr.payment_timestamp = fact.payment_timestamp 	)
  and (tr.payment_date = fact.payment_date 	)
  and (tr.order_type = fact.order_type 	)
  and (tr.authentication_code = fact.authentication_code 	or (tr.authentication_code is null and fact.authentication_code is null))
  and (tr.virtual_account = fact.virtual_account 	or (tr.virtual_account is null and fact.virtual_account is null))
  and (tr.giftcard_voucher = fact.giftcard_voucher 	or (tr.giftcard_voucher is null and fact.giftcard_voucher is null))
  and (tr.promocode_name = fact.promocode_name 	or (tr.promocode_name is null and fact.promocode_name is null))
  and (tr.payment_source = fact.payment_source 	or (tr.payment_source is null and fact.payment_source is null))
  and (tr.payment_gateway = fact.payment_gateway 	or (tr.payment_gateway is null and fact.payment_gateway is null))
  and (tr.payment_type_bank = fact.payment_type_bank 	or (tr.payment_type_bank is null and fact.payment_type_bank is null))
  and (tr.payment_order_name = fact.payment_order_name 	or (tr.payment_order_name is null and fact.payment_order_name is null))
  and (tr.payment_charge = fact.payment_charge 	or (tr.payment_charge is null and fact.payment_charge is null))
  and (tr.promocode_value = fact.promocode_value 	or (tr.promocode_value is null and fact.promocode_value is null))
  and (tr.giftvoucher_value = fact.giftvoucher_value 	or (tr.giftvoucher_value is null and fact.giftvoucher_value is null))
  and (tr.refund_deposit_value = fact.refund_deposit_value 	or (tr.refund_deposit_value is null and fact.refund_deposit_value is null))
  and (tr.tiketpoint_value = fact.tiketpoint_value 	or (tr.tiketpoint_value is null and fact.tiketpoint_value is null))
  and (tr.insurance_value = fact.insurance_value 	or (tr.insurance_value is null and fact.insurance_value is null))
  and (tr.cancel_insurance_value = fact.cancel_insurance_value 	or (tr.cancel_insurance_value is null and fact.cancel_insurance_value is null))
  and (tr.giftvoucher_name = fact.giftvoucher_name 	or (tr.giftvoucher_name is null and fact.giftvoucher_name is null))
  and (tr.refund_deposit_name = fact.refund_deposit_name 	or (tr.refund_deposit_name is null and fact.refund_deposit_name is null))
  and (tr.insurance_name = fact.insurance_name 	or (tr.insurance_name is null and fact.insurance_name is null))
  and (tr.cancel_insurance_name = fact.cancel_insurance_name 	or (tr.cancel_insurance_name is null and fact.cancel_insurance_name is null))
  and (tr.pg_charge = fact.pg_charge 	or (tr.pg_charge is null and fact.pg_charge is null))
  and (tr.cc_installment = fact.cc_installment 	or (tr.cc_installment is null and fact.cc_installment is null))
  and (tr.order_name = fact.order_name 	)
  and (tr.order_name_detail = fact.order_name_detail 	)
  and (tr.insurance_issue_code = fact.insurance_issue_code 	or (tr.insurance_issue_code is null and fact.insurance_issue_code is null))
  and (tr.cancel_insurance_issue_code = fact.cancel_insurance_issue_code 	or (tr.cancel_insurance_issue_code is null and fact.cancel_insurance_issue_code is null))
  and (tr.acquiring_bank = fact.acquiring_bank 	or (tr.acquiring_bank is null and fact.acquiring_bank is null))
  and (tr.cogs = fact.cogs 	or (tr.cogs is null and fact.cogs is null))
  and (tr.quantity = fact.quantity 	or (tr.quantity is null and fact.quantity is null))
  and (tr.commission = fact.commission 	or (tr.commission is null and fact.commission is null))
  and (tr.upselling = fact.upselling 	or (tr.upselling is null and fact.upselling is null))
  and (tr.subsidy = fact.subsidy 	or (tr.subsidy is null and fact.subsidy is null))
  and (tr.product_category = fact.product_category 	or (tr.product_category is null and fact.product_category is null))
  and (tr.rebooking_price_hotel = fact.rebooking_price_hotel or (tr.rebooking_price_hotel is null and fact.rebooking_price_hotel is null))
  and (tr.rebooking_sales_hotel = fact.rebooking_sales_hotel or (tr.rebooking_sales_hotel is null and fact.rebooking_sales_hotel is null))
  and (tr.supplier = fact.supplier 	or (tr.supplier is null and fact.supplier is null))
  and (tr.product_provider = fact.product_provider 	or (tr.product_provider is null and fact.product_provider is null))
  and (tr.revenue_category = fact.revenue_category 	)
  and (tr.vat_out = fact.vat_out 	or (tr.vat_out is null and fact.vat_out is null))
  and (tr.baggage_fee = fact.baggage_fee 	or (tr.baggage_fee is null and fact.baggage_fee is null))
  and (tr.flight_reschedule_old_order_detail_id = fact.flight_reschedule_old_order_detail_id 	or (tr.flight_reschedule_old_order_detail_id is null and fact.flight_reschedule_old_order_detail_id is null))
  and (tr.product_provider_reschedule_flight = fact.product_provider_reschedule_flight 	or (tr.product_provider_reschedule_flight is null and fact.product_provider_reschedule_flight is null))
  and (tr.supplier_reschedule_flight = fact.supplier_reschedule_flight 	or (tr.supplier_reschedule_flight is null and fact.supplier_reschedule_flight is null))
  and (tr.reschedule_fee_flight = fact.reschedule_fee_flight )
  and (tr.reschedule_cashback_amount = fact.reschedule_cashback_amount 	)
  and (tr.reschedule_promocode_amount = fact.reschedule_promocode_amount 	)
  and (tr.refund_amount_flight = fact.refund_amount_flight 	)
  and (tr.reschedule_miscellaneous_amount = fact.reschedule_miscellaneous_amount 	)
  and (tr.booking_code = fact.booking_code 	or (tr.booking_code is null and fact.booking_code is null))
  and (tr.ticket_number = fact.ticket_number 	or (tr.ticket_number is null and fact.ticket_number is null))
  and (tr.memo_product = fact.memo_product 	or (tr.memo_product is null and fact.memo_product is null))
  and (tr.memo_hotel = fact.memo_hotel 	or (tr.memo_hotel is null and fact.memo_hotel is null))
  and (tr.memo_flight = fact.memo_flight 	or (tr.memo_flight is null and fact.memo_flight is null))
  and (tr.memo_cancel_insurance = fact.memo_cancel_insurance 	or (tr.memo_cancel_insurance is null and fact.memo_cancel_insurance is null))
  and (tr.memo_insurance = fact.memo_insurance 	or (tr.memo_insurance is null and fact.memo_insurance is null))
  and (tr.issued_status = fact.issued_status 	or (tr.issued_status is null and fact.issued_status is null))
  and (tr.selling_price_proportion_value = fact.selling_price_proportion_value 	or (tr.selling_price_proportion_value is null and fact.selling_price_proportion_value is null))
  and (tr.all_issued_flag = fact.all_issued_flag 	)
  and (tr.event_data_error_flag = fact.event_data_error_flag 	)
  and (tr.pay_at_hotel_flag = fact.pay_at_hotel_flag 	)
  and (tr.new_supplier_flag = fact.new_supplier_flag 	)
  and (tr.new_product_provider_flag = fact.new_product_provider_flag 	)
  and (tr.new_b2b_online_and_offline_flag = fact.new_b2b_online_and_offline_flag 	)
  and (tr.new_b2b_corporate_flag = fact.new_b2b_corporate_flag 	)
  and (tr.is_supplier_flight_not_found_flag = fact.is_supplier_flight_not_found_flag 	)
  and (tr.add_ons_hotel_detail_json = fact.add_ons_hotel_detail_json 	or (tr.add_ons_hotel_detail_json is null and fact.add_ons_hotel_detail_json is null))
  and (tr.halodoc_sell_price_amount = fact.halodoc_sell_price_amount)
  and (tr.halodoc_pax_count = fact.halodoc_pax_count or (tr.halodoc_pax_count is null and fact.halodoc_pax_count is null))
  and (tr.memo_halodoc = fact.memo_halodoc or (tr.memo_halodoc is null and fact.memo_halodoc is null))
  and (tr.is_has_halodoc_flag = fact.is_has_halodoc_flag or (tr.is_has_halodoc_flag is null and fact.is_has_halodoc_flag is null))
  and coalesce(tr.convenience_fee_amount,0) = coalesce(tr.convenience_fee_amount,0)
  and (tr.memo_convenience_fee = fact.memo_convenience_fee or (tr.memo_convenience_fee is null and fact.memo_convenience_fee is null))
  and (tr.giftcard_voucher_user_email_reference_id = fact.giftcard_voucher_user_email_reference_id or (tr.giftcard_voucher_user_email_reference_id is null and fact.giftcard_voucher_user_email_reference_id is null))
  and (tr.giftcard_voucher_purpose = fact.giftcard_voucher_purpose or (tr.giftcard_voucher_purpose is null and fact.giftcard_voucher_purpose is null))
  and (tr.memo_giftvoucher = fact.memo_giftvoucher or (tr.memo_giftvoucher is null and fact.memo_giftvoucher is null))
  and (tr.old_id_rebooking = fact.old_id_rebooking or (tr.old_id_rebooking is null and fact.old_id_rebooking is null))
  and (tr.diff_amount_rebooking = fact.diff_amount_rebooking or (tr.diff_amount_rebooking is null and fact.diff_amount_rebooking is null))
  and (tr.is_rebooking_flag = fact.is_rebooking_flag or (tr.is_rebooking_flag is null and fact.is_rebooking_flag is null))
where
  tr.order_id is null
)

select * from append