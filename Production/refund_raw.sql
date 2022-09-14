with
fd as (
  select
    filter1
    , date_add(filter1, interval -3 day) as filter2
    , date_add(filter1, interval -365 day) as filter3
  from
  (
    select
      timestamp_add(timestamp(date(current_timestamp(),'Asia/Jakarta')), interval -31 hour) as filter1
  )
)
, lsw as (
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
, amd as (
  select
    distinct
    order_id
    , extract(year FROM payment_date) as invoice_year
  from
    `datamart-finance.staging.v_all_management_dashboard`
)
, afd as (
  select
    safe_cast(order_id as int64) as order_id
    , safe_cast(order_detail_id as int64) as order_detail_id
    , kodebook as booking_code
    , sum(safe_cast(regexp_replace(nominal, '[^0-9 ]','') as float64)) as refund_flight_amount
    , parse_date('%d-%b-%y', tanggal_refund_masuk) as request_date
  from
    `datamart-finance.staging.v_airlines_refund_deposit`
  group by 1,2,3,5,processed_dttm
)
, ocf as (
  select
    order_detail_id
    , if(trip = 'roundtrip', true, false) as is_roundtrip
  from
    `datamart-finance.staging.v_order__cart_flight`
)
, tro as (
  select
    coalesce(referenceId, refundOptOrderId) as order_id
    , refundRequestDateTime as tro_refund_request_date
  from
    `datamart-finance.staging.v_tix_refund_refund_order`
  where
    date(refundRequestDateTime,'Asia/Jakarta')
      between date((select filter2 from fd),'Asia/Jakarta') and date((select filter1 from fd),'Asia/Jakarta')
)
, hcb as (
  select
    order_id
    , charge
    , from_deadline
    , to_deadline
    , after_deadline
    , if
        (
          if
            (
              charge <> 0 and after_deadline is null and tro_refund_request_date > to_deadline
              , true
              , ifnull(tro_refund_request_date > from_deadline, true) and tro_refund_request_date <= to_deadline
            )
          , tro_refund_request_date
          , null
        )
      as refund_request_date
  from
  (
  select
    safe_cast(OrderId as int64) as order_id
    , policies.element.charge
    , timestamp
      (
      lag
        (
          replace(policies.element.deadline, 'UTC', '')
        )
      over
        (
          partition by OrderId order by replace(policies.element.deadline, 'UTC', '')
        )
      ) as from_deadline
    , timestamp(replace(policies.element.deadline, 'UTC', '')) as to_deadline
    , timestamp
      (
      lead
        (
          replace(policies.element.deadline, 'UTC', '')
        )
      over
        (
          partition by OrderId order by replace(policies.element.deadline, 'UTC', '')
        )
      ) as after_deadline
    , tro_refund_request_date
  from
    tro join `datamart-finance.staging.v_hotel_cart_book` on order_id = safe_cast(OrderId as int64)
    left join unnest(detail_room.cancellationPoliciesV3.policies.list) as policies
  where
    createdDate >= (select filter3 from fd)
  )
)
, hpt as (
  select
    distinct
    itinerary_id
    , payment_status
  from
    `datamart-finance.staging.v_hotel_payments`
  qualify row_number() over(partition by itinerary_id order by processed_dttm desc) = 1
)
, supplier as (
  select
    *
  from
  (
    select
      distinct
      workday_supplier_reference_id as supplier_reference_id
      , workday_supplier_name as supplier_name
      , vendor
    from
      `datamart-finance.staging.v_workday_mapping_supplier`
    where
      workday_supplier_reference_id is not null
    union distinct
    select
      distinct
      Supplier_Reference_ID as supplier_reference_id
      , Supplier_Name as supplier_name
      , '' as vendor
    from
      `datamart-finance.datasource_workday.master_data_supplier`
    where Supplier_Reference_ID not in
    (
      select
        distinct workday_supplier_reference_id
      from
        `datamart-finance.staging.v_workday_mapping_supplier`
      where
        workday_supplier_reference_id is not null
    )
    and Supplier_Name is not null
  )
  union all
  select 'VR-00000023' as supplier_reference_id, 'EXPEDIA' as supplier_name, '' as vendor union all
  select 'VR-00000024' as supplier_reference_id, 'HOTELBEDS' as supplier_name, '' as vendor union all
  select 'VR-00000025' as supplier_reference_id, 'AGODA' as supplier_name, '' as vendor
)
, rff as (
  select
    coalesce(referenceId, refundOptOrderId) as order_id
    , coalesce(referenceDetailId, refundOptOrderDetailId) as order_detail_id
    , refundId as refund_id
    , trim(bookingCode) as refund_booking_code
    , refundType as refund_type
    , datetime(refundRequestDateTime,'Asia/Jakarta') as refund_request_date
    , datetime(updatedDate,'Asia/Jakarta') as refunded_date
    , timestamp_add(datetime(refundRequestDateTime,'Asia/Jakarta'), interval 14 day) as due_date
    , refundAmount as refund_amount
    , estimateAirlinesRefund as estimated_refund_flight
    , refund_flight_amount
    , ifnull(charge,0) as hotel_cancellation_fee
    , isReschedule as is_reschedule
    , partiallyRefund as is_partially_refund
    , trim(refundSplitCode) as refund_split_code
    , refundPaymentType as refund_payment_type
    , orderPaymentType as order_payment_type
    , refundBankAccount as refund_bank_account
    , refundCustName as refund_cust_name
    , trim(refundReason) as refund_reason
    , orderName
    , ifnull(countAdult,0)+ifnull(countChild,0) as total_pax
    , if((trim(refundReason) in ('Alasan Pribadi','Personal Reason') or (trim(refundReason) like '%Pengajuan Pribadi%'))
      , -1*if(is_roundtrip,50000,25000)*(ifnull(countAdult,0)+ifnull(countChild,0))
      , 0) as refund_fee
    , case when orderPaymentType <> 'affiliate_deposit' then -1*ifnull(adminFee, 0)
      else 0 end as admin_fee
    , refundStatus as refund_status
  from
    `datamart-finance.staging.v_tix_refund_refund_order` as tro
  left join afd
    on (
      coalesce(referenceId, refundOptOrderId) = safe_cast(afd.order_id as int64)
      and coalesce(referenceDetailId, refundOptOrderDetailId) = safe_cast(afd.order_detail_id as int64)
      and (
            date(datetime(createdDate,'Asia/Jakarta')) = request_date
            or
            date(datetime(refundRequestDateTime,'Asia/Jakarta')) = request_date
          )
      )
    or (
      coalesce(referenceId, refundOptOrderId) = safe_cast(afd.order_id as int64)
      and coalesce(referenceDetailId, refundOptOrderDetailId) = safe_cast(afd.order_detail_id as int64)
      and trim(bookingCode) = trim(booking_code)
    )
  left join hcb
    on coalesce(referenceId, refundOptOrderId) = hcb.order_id
    and date(datetime(refundRequestDateTime,'Asia/Jakarta')) = date(datetime(refund_request_date,'Asia/Jakarta'))
  left join ocf
    using(order_detail_id)
  left join lsw
    on coalesce(referenceId, refundOptOrderId) = lsw.order_id
    and refundId = lsw.order_detail_id
  where
    lower(refundStatus) <> 'rejected'
    and refundPaymentType not in ('AIRLINES_VOUCHER', 'TIX_POINT')
    and refundAmount <> 0
    and (
          lower(regexp_replace(refundCustName, '[^0-9a-zA-Z]+', '')) not like 'ptglobaltiketnetwork%'
          or refundCustName is null
         )
    and (
          lower(partnerStatus) like '%failed to validate json schema%'
          or lower(partnerStatus) not like '%fail%'
          or partnerStatus is null
        )
    and date(refundRequestDateTime,'Asia/Jakarta')
      between date((select filter2 from fd),'Asia/Jakarta') and date((select filter1 from fd),'Asia/Jakarta')
    and is_sent_flag is null
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
)
, ci as (
  select 
    * except(rn)
    , sum(tiketpoint_value) over(partition by order_id) as total_tiketpoint
  from
  (
  select
    order_id
    , order_detail_id
    , payment_timestamp
    , booking_code
    , quantity
    , commission+ifnull(total_add_ons_hotel_commission_amount,0) as commission
    , partner_commission
    , subsidy
    , subsidy_category
    , upselling
    , if(
          selling_price_proportion_value < 1
          , round(safe_divide(giftvoucher_value,selling_price_proportion_value))/count(giftvoucher_value) over(partition by order_id, processed_timestamp)
          , giftvoucher_value
        )
      as giftvoucher_value
    , if(
          selling_price_proportion_value < 1
          , round(safe_divide(tiketpoint_value,selling_price_proportion_value))/count(tiketpoint_value) over(partition by order_id, processed_timestamp)
          , tiketpoint_value
        )
      as tiketpoint_value
    , if(
          selling_price_proportion_value < 1
          , round(safe_divide(promocode_value,selling_price_proportion_value))/count(promocode_value) over(partition by order_id, processed_timestamp)
          , promocode_value
        )
      as promocode_value
    , payment_type_bank
    , payment_charge
    , payment_amount
    , total_ancillary_flight
    , insurance_value
    , case
        when order_type in ('event', 'car') and product_category is null and cogs = 0 then false
        when issued_status = 'issued' then true
        else false
      end as is_issued_flag
    , row_number() over(partition by order_id, order_detail_id order by processed_timestamp desc) as rn
  from
    `datamart-finance.datamart_edp.customer_invoice_raw_2021`
  )
  where rn = 1
)
, si as (
  select
    * except(total_line_amount)
    , sum(total_line_amount) as si_amount
  from
  (
  select 
    * except(rn)
  from
  (
  select
    Company
    , order_id
    , order_detail_id
    , case when deposit_flag = 'Deposit' then true
      else false end as is_deposit_flag
    , product_category
    , supplier_reference_id
    , product_provider
    , order_detail_name
    , memo
    , total_line_amount
    , invoice_currency
    , currency_conversion
    , customer_reference_id
    , row_number() over(partition by order_id, order_detail_id,spend_category,tier_code order by processed_timestamp desc) as rn
  from
    `datamart-finance.datamart_edp.supplier_invoice_raw_OTW`
  left join
    unnest (json_extract_array(array_reverse(split(booking_code,' - '))[safe_offset(0)])) as tier_code
  where
    total_line_amount is not null
    and spend_category not in ('Bagage', 'Seat_Flight')
  )
  where rn = 1
  )
  group by 1,2,3,4,5,6,7,8,9,10,11,12
)
, combine as (
  select
    * except(total_pax, quantity, hotel_cancellation_fee, insurance_value, admin_fee, memo)
    , total_pax as refund_pax
    , safe_cast(if(quantity=0,1,quantity) as int64) as sales_pax
    , case
        when hotel_cancellation_fee = refund_amount and hotel_cancellation_fee/if(payment_amount=0,1,payment_amount) <> 0.5 then 0
        when
          hotel_cancellation_fee
          + if(lower(refund_payment_type) = 'bank_transfer' and product_category in ('Hotel', 'Hotel_NHA'),-5000,admin_fee)
          = refund_amount
          and hotel_cancellation_fee/if(payment_amount=0,1,payment_amount) <> 0.5
          then 0
        when hotel_cancellation_fee = (si_amount * currency_conversion) then 0
        when payment_amount + ifnull(admin_fee, 0) = refund_amount then 0
        else hotel_cancellation_fee
      end as hotel_cancellation_fee
    , case when lower(refund_payment_type) = 'bank_transfer' and product_category in ('Hotel', 'Hotel_NHA') then -5000
      else admin_fee end as admin_fee
    , if(product_category='Hotel_NHA', insurance_value, 0) as insurance_value
    , case when product_category is not null then
        concat(
          order_id
          , '_'
          , order_detail_id
          , '_'
          , ifnull(booking_code, '')
          , '_'
          , ifnull(supplier_name, '')
          , '_'
          , ifnull(order_detail_name, '')
        )
      else memo end as memo
  from
    rff
    left join amd using(order_id)
    left join ci using(order_id, order_detail_id)
    left join si using(order_id, order_detail_id)
    left join supplier using(supplier_reference_id)
    left join hpt on safe_cast(ci.booking_code as int64) = hpt.itinerary_id
)
, fact1 as ( /* sum all breakdown without payment charge */
  select
    ifnull(Company, 'GTN_IDN') as Company 
    , ifnull(invoice_currency, 'IDR') as invoice_currency
    , ifnull(currency_conversion, safe_cast(1 as float64)) as currency_conversion
    , case
        when payment_type_bank is null and is_issued_flag is null then 'Refund_Flip'
        when lower(payment_type_bank) = 'pg_affiliate_deposit'
          then
            if
              (
                lower(trim(array_reverse(split(refund_bank_account, ' '))[safe_offset(0)])) = 'cermati'
                or
                lower(trim(refund_cust_name)) = 'cermati'
                , 'Refund_Credit_Card_BCA'
                , 'Refund_Flip'
              )
        when lower(payment_type_bank) = 'pg_credit_card_bca' then 'Refund_Credit_Card_BCA'
        when lower(refund_payment_type) = 'bank_transfer' and lower(payment_type_bank) not in
          (
            'pg_credit_card_cimb'
            , 'pg_kredivo'
            , 'pg_credit_card_bni'
            , 'pg_credit_card_mandiri'
            , 'pg_akulaku'
            , 'pg_indodana'
            , 'pg_credit_card_bri'
          )
          then 'Refund_Flip'
        else
        if
          (
            lower(payment_type_bank) in
            (
              'pg_credit_card_bca',
              'pg_credit_card_bni',
              'pg_credit_card_bri',
              'pg_credit_card_cimb',
              'pg_credit_card_mandiri',
              'pg_indodana',
              'pg_kredivo',
              'pg_flip',
              'pg_akulaku'
            )
            , replace(payment_type_bank, 'PG_', 'Refund_')
            , ''
          )
      end as supplier_reference_id
    , payment_timestamp as invoice_date
    , invoice_year
    , refund_request_date
    , refunded_date
    , due_date
    , ifnull(is_partially_refund, false) as is_partially_refund
    , refund_id
    , refund_type
    , refund_reason
    , order_id
    , order_detail_id
    , refund_pax
    , sales_pax
    , case when (not is_issued_flag or is_issued_flag is null) and refund_amount <> 0
        then refund_booking_code
      else booking_code end as booking_code
    , refund_split_code
    , if(vendor='sa', true, false) as is_sabre
    , is_reschedule
    , case when (not is_issued_flag or is_issued_flag is null or product_category = 'Train') and refund_amount <> 0 then 'Others'
      else product_category end as product_category
    , case when (not is_issued_flag or is_issued_flag is null or product_category = 'Train') and refund_amount <> 0 then 'ProductOthers'
      else product_provider end as product_provider
    , refund_amount
    , if(product_category='Flight',coalesce(refund_flight_amount,estimated_refund_flight,0)
      , case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*(si_amount * currency_conversion))           else round(si_amount * currency_conversion) end) +
      refund_fee +
      if(lower(payment_type_bank) = 'pg_indodana', 0, admin_fee) +
      case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*commission)
      when is_partially_refund and refund_pax > 0 and commission is not null then round(refund_pax/safe_cast(sales_pax as int64)*commission)
      else ifnull(commission, 0) end +
      case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*partner_commission)
      when is_partially_refund and refund_pax > 0 and partner_commission is not null then round(refund_pax/safe_cast(sales_pax as int64)*partner_commission)
      else ifnull(partner_commission, 0) end +
      case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*subsidy)
      when is_partially_refund and refund_pax > 0 and subsidy is not null then round(refund_pax/safe_cast(sales_pax as int64)*subsidy)
      else ifnull(subsidy, 0) end +
      case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*upselling)
      when is_partially_refund and refund_pax > 0 and upselling is not null then round(refund_pax/safe_cast(sales_pax as int64)*upselling)
      else ifnull(upselling, 0) end +
      case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*giftvoucher_value)
      when is_partially_refund and refund_pax > 0 and giftvoucher_value is not null then round(refund_pax/safe_cast(sales_pax as int64)*giftvoucher_value)
      else ifnull(giftvoucher_value,0) end +
      case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*tiketpoint_value)
      when is_partially_refund and refund_pax > 0 and tiketpoint_value is not null then round(refund_pax/safe_cast(sales_pax as int64)*tiketpoint_value)
      else ifnull(tiketpoint_value, 0) end +
      case when is_partially_refund and refund_pax > 0 and promocode_value is not null then round(refund_pax/safe_cast(sales_pax as int64)*promocode_value)
      else ifnull(promocode_value, 0) end +
      case when is_partially_refund and refund_pax > 0 and insurance_value is not null then round(refund_pax/safe_cast(sales_pax as int64)*insurance_value)
      else ifnull(insurance_value, 0) end
      as total_breakdown_amount
    , if(refund_amount = coalesce(refund_flight_amount,estimated_refund_flight,0), true, false) as is_topup_match
    , refund_flight_amount
    , estimated_refund_flight
    , if(payment_status = 'PAID', true, false) as is_hotel_paid
    , hotel_cancellation_fee
    , case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*(si_amount * currency_conversion))
      else round(si_amount * currency_conversion) end as si_amount
    , refund_fee
    , if(lower(payment_type_bank) = 'pg_indodana', 0, admin_fee) as admin_fee
    , case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*commission)
      when is_partially_refund and refund_pax > 0 and commission is not null then round(refund_pax/safe_cast(sales_pax as int64)*commission)
      else commission end as commission
    , partner_commission
    , case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*subsidy)
      when is_partially_refund and refund_pax > 0 and subsidy is not null then round(refund_pax/safe_cast(sales_pax as int64)*subsidy)
      else subsidy end as subsidy
    , subsidy_category
    , case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*upselling)
      when is_partially_refund and refund_pax > 0 and upselling is not null then round(refund_pax/safe_cast(sales_pax as int64)*upselling)
      else upselling end as upselling
    , case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*giftvoucher_value)
      when is_partially_refund and refund_pax > 0 and giftvoucher_value is not null then round(refund_pax/safe_cast(sales_pax as int64)*giftvoucher_value)
      else giftvoucher_value end as giftvoucher_value
    , case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*tiketpoint_value)
      when is_partially_refund and refund_pax > 0 and tiketpoint_value is not null then round(refund_pax/safe_cast(sales_pax as int64)*tiketpoint_value)
      else tiketpoint_value end as tiketpoint_value
    , total_tiketpoint
    , case when is_partially_refund and refund_pax > 0 and promocode_value is not null then round(refund_pax/safe_cast(sales_pax as int64)*promocode_value)
      else promocode_value end as promocode_value
    , insurance_value
    , case when hotel_cancellation_fee <> 0 and payment_amount <> 0 then round((1-(hotel_cancellation_fee/(payment_amount-promocode_value)))*payment_charge)
      when is_partially_refund and refund_pax > 0 and payment_charge is not null then round(refund_pax/safe_cast(sales_pax as int64)*payment_charge)
      else payment_charge end as payment_charge
    , payment_amount
    , total_ancillary_flight
    , order_payment_type
    , is_deposit_flag
    , case when (not is_issued_flag or is_issued_flag is null) and refund_amount <> 0 then safe_cast(refund_booking_code as string)
      else trim(order_detail_name) end as order_detail_name
    , case when (not is_issued_flag or is_issued_flag is null) and refund_amount <> 0 then safe_cast(
        concat(order_id
              , if(refund_booking_code = '' or refund_booking_code is null, '', '_')
              , if(refund_booking_code = '' or refund_booking_code is null, '', refund_booking_code))
        as string)
      else trim(memo) end as memo
    , customer_reference_id
    , null as additional_info_json
    , ifnull(is_issued_flag, false) as is_issued_flag
    , refund_status
  from
    combine
)
, fact2 as ( /* combine the value of divided tiketpoints */
  select
    *
    , refund_amount - total_breakdown_amount as diff_refund
    , total_tiketpoint - tiketpoint_value as proportion_tiketpoint
    , case
      when
      (refund_amount - total_breakdown_amount <> 0)
      and
      (total_tiketpoint - tiketpoint_value <> 0)
      and
      not is_partially_refund
        then
          if
          (
            (refund_amount - total_breakdown_amount = total_tiketpoint - tiketpoint_value)
            , total_tiketpoint
            , 0
          )
      else null end as new_tiketpoint_value
  from
    fact1
)
, fact3 as ( /* set the new value of tiketpoints and set zero to payment charge */
  select
    * except(total_breakdown_amount, diff_refund, tiketpoint_value, payment_charge)
    , case when diff_refund <> 0 and new_tiketpoint_value is not null
        then total_breakdown_amount - tiketpoint_value + new_tiketpoint_value
      else total_breakdown_amount end as total_breakdown_amount
    , case when diff_refund <> 0 and new_tiketpoint_value is not null
        then refund_amount - (total_breakdown_amount - tiketpoint_value + new_tiketpoint_value)
      else diff_refund end as diff_refund
    , case when diff_refund <> 0 and new_tiketpoint_value is not null then new_tiketpoint_value
      else tiketpoint_value end as tiketpoint_value
    , case when diff_refund = 0 then 0
      else payment_charge end as payment_charge
  from
    fact2
)
, fact4 as ( /* Set up order only refund amount with admin fee/refund fee */
  select
    * except
        (
          refund_flight_amount
          , estimated_refund_flight
          , si_amount
          , commission
          , subsidy
          , subsidy_category
          , upselling
          , giftvoucher_value
          , tiketpoint_value
          , promocode_value
          , payment_charge
          , admin_fee
          , total_breakdown_amount
          , diff_refund
          , product_category
          , product_provider
        )
    , case when refund_reason = 'Customer Double Paid'
        then if(product_category = 'Flight', refund_amount, 0)
      else refund_flight_amount end as refund_flight_amount
    , case when refund_reason = 'Customer Double Paid'
        then if(product_category <> 'Flight', refund_amount, 0)
      else si_amount end as si_amount  
    , case when refund_reason = 'Customer Double Paid'
        then 0
      else estimated_refund_flight end as estimated_refund_flight
    , case when refund_reason = 'Customer Double Paid'
        then 0
      else commission end as commission
    , case when refund_reason = 'Customer Double Paid'
        then 0
      else subsidy end as subsidy
    , case when refund_reason = 'Customer Double Paid'
        then null
      else subsidy_category end as subsidy_category
    , case when refund_reason = 'Customer Double Paid'
        then 0
      else upselling end as upselling
    , case when refund_reason = 'Customer Double Paid'
        then 0
      else giftvoucher_value end as giftvoucher_value
    , case when refund_reason = 'Customer Double Paid'
        then 0
      else tiketpoint_value end as tiketpoint_value
    , case when refund_reason = 'Customer Double Paid'
        then 0
      else promocode_value end as promocode_value
    , case when refund_reason = 'Customer Double Paid'
        then 0
      else payment_charge end as payment_charge
    , case when refund_reason = 'Customer Double Paid' and is_reschedule then 0
      else admin_fee end as admin_fee
    , case when refund_reason = 'Customer Double Paid'
        then refund_amount + if(not is_reschedule, admin_fee, 0) + refund_fee
      else total_breakdown_amount end as total_breakdown_amount
    , case when refund_reason = 'Customer Double Paid'
        then 0
      else diff_refund end as diff_refund
    , case when refund_reason = 'Customer Double Paid' then 'Others'
      else product_category end as product_category
    , case when refund_reason = 'Customer Double Paid' then 'ProductOthers'
      else product_provider end as product_provider
  from
    fact3
)
, fact5 as ( /* remove commission */
  select
    * except(total_breakdown_amount, diff_refund, commission)
    , case when diff_refund <> 0 and diff_refund + commission = 0
        then total_breakdown_amount - commission
      else total_breakdown_amount end as total_breakdown_amount
    , case when diff_refund <> 0 and diff_refund + commission = 0
        then refund_amount - (total_breakdown_amount - commission)
      else diff_refund end as diff_refund
    , case when diff_refund <> 0 and diff_refund + commission = 0 then 0
      else commission end as commission
  from
    fact4
)
, fact6 as ( /* remove subsidy */
  select
    * except(total_breakdown_amount, diff_refund, subsidy)
    , case when diff_refund <> 0 and diff_refund + subsidy = 0
        then total_breakdown_amount - subsidy
      else total_breakdown_amount end as total_breakdown_amount
    , case when diff_refund <> 0 and diff_refund + subsidy = 0
        then refund_amount - (total_breakdown_amount - subsidy)
      else diff_refund end as diff_refund
    , case when diff_refund <> 0 and diff_refund + subsidy = 0 then 0
      else subsidy end as subsidy
  from
    fact5
)
, fact7 as ( /* remove commission and subsidy */
  select
    * except(total_breakdown_amount, diff_refund, commission, subsidy)
    , case when diff_refund <> 0 and refund_amount - (total_breakdown_amount - (commission+subsidy)) = 0
        then total_breakdown_amount - (commission+subsidy)
      else total_breakdown_amount end as total_breakdown_amount
    , case when diff_refund <> 0 and refund_amount - (total_breakdown_amount - (commission+subsidy)) = 0
        then refund_amount - (total_breakdown_amount - (commission+subsidy)) + ifnull(payment_charge,0)
      else diff_refund end as diff_refund
    , case when diff_refund <> 0 and refund_amount - (total_breakdown_amount - (commission+subsidy)) = 0 then 0
      else commission end as commission
    , case when diff_refund <> 0 and refund_amount - (total_breakdown_amount - (commission+subsidy)) = 0 then 0
      else subsidy end subsidy
  from
    fact6
)
, fact8 as ( /* add payment charge */
  select
    * except(total_breakdown_amount, diff_refund)
    , case when diff_refund <> 0 and payment_charge = diff_refund
        then total_breakdown_amount + payment_charge
      else total_breakdown_amount end as total_breakdown_amount
    , case when diff_refund <> 0 and payment_charge = diff_refund
        then refund_amount - (total_breakdown_amount + payment_charge)
      else diff_refund end as diff_refund
  from
    fact7
)
, fact9 as ( /* add diff in multiples of the refund fee (25000) and set zero to payment charge */
  select
    * except(refund_fee, commission, total_breakdown_amount, diff_refund, payment_charge)
    , case
        when
          refund_amount - (total_breakdown_amount - commission) <> 0
          and
          mod(safe_cast(refund_amount - (total_breakdown_amount - commission) as int64), 25000) = 0
          and
          refund_fee + (refund_amount - (total_breakdown_amount - commission)) < 0
          then refund_fee + (refund_amount - (total_breakdown_amount - commission))
        else refund_fee
      end as refund_fee
    , case
        when
          refund_amount - (total_breakdown_amount - commission) <> 0
          and
          mod(safe_cast(refund_amount - (total_breakdown_amount - commission) as int64), 25000) = 0
          and
          refund_fee + (refund_amount - (total_breakdown_amount - commission)) < 0
          then 0
        else commission end as commission
    , case
        when
          refund_amount - (total_breakdown_amount - commission) <> 0
          and
          mod(safe_cast(refund_amount - (total_breakdown_amount - commission) as int64), 25000) = 0
          and
          refund_fee + (refund_amount - (total_breakdown_amount - commission)) < 0
          then total_breakdown_amount - refund_fee - commission + (refund_fee + (refund_amount - (total_breakdown_amount - commission)))
        else total_breakdown_amount end as total_breakdown_amount
    , case
        when
          refund_amount - (total_breakdown_amount - commission) <> 0
          and
          mod(safe_cast(refund_amount - (total_breakdown_amount - commission) as int64), 25000) = 0
          and
          refund_fee + (refund_amount - (total_breakdown_amount - commission)) < 0
          then 0
        else payment_charge end as payment_charge
    , case
        when
          refund_amount - (total_breakdown_amount - commission) <> 0
          and
          mod(safe_cast(refund_amount - (total_breakdown_amount - commission) as int64), 25000) = 0
          and
          refund_fee + (refund_amount - (total_breakdown_amount - commission)) < 0
          then refund_amount - (total_breakdown_amount - refund_fee - commission + (refund_fee + (refund_amount - (total_breakdown_amount - commission))))
       else diff_refund end as diff_refund
  from
    fact8
)
, fact10 as ( /* Add diff of admin fee (5000) and set zero to payment charge */
  select
    * except(admin_fee, total_breakdown_amount, diff_refund, payment_charge)
    , case
        when
          diff_refund in (-5000,5000) and admin_fee + diff_refund <= 0
          then admin_fee + diff_refund
        else admin_fee
      end as admin_fee
    , case
        when
          diff_refund in (-5000,5000) and admin_fee + diff_refund <= 0
          then total_breakdown_amount + diff_refund
        else total_breakdown_amount end as total_breakdown_amount
    , case
        when
          diff_refund in (-5000,5000) and admin_fee + diff_refund <= 0
          then 0
        else payment_charge end as payment_charge
    , case
        when
          diff_refund in (-5000,5000) and admin_fee + diff_refund <= 0
          then refund_amount - (total_breakdown_amount + diff_refund)
       else diff_refund end as diff_refund
  from
    fact9
)
, fact11 as ( /* Set up order not issued */
  select
    Company
    , invoice_currency
    , currency_conversion
    , supplier_reference_id
    , invoice_date
    , invoice_year
    , refund_request_date
    , refunded_date
    , due_date
    , is_partially_refund
    , refund_id
    , refund_type
    , refund_reason
    , order_id
    , order_detail_id
    , case when product_category = 'Others' then if(refund_pax = 0 or refund_pax is null, 1, refund_pax)
      else refund_pax end as refund_pax
    , case when product_category = 'Others' then if(sales_pax = 0 or sales_pax is null, 1, sales_pax)
      else sales_pax end as sales_pax
    , booking_code
    , refund_split_code
    , is_sabre
    , is_reschedule
    , product_category
    , product_provider
    , case when product_category = 'Others' then refund_amount+coalesce(-1*admin_fee,0)+coalesce(-1*refund_fee,0)
      else refund_amount end as refund_amount
    , total_breakdown_amount
    , case when product_category = 'Others' then 0
      else diff_refund end as diff_refund
    , case when product_category = 'Others' then 0
      else refund_flight_amount end as refund_flight_amount
    , case when product_category = 'Others' then 0
      else estimated_refund_flight end as estimated_refund_flight
    , case when product_category = 'Others' then false
      else is_hotel_paid end as is_hotel_paid
    , case when product_category = 'Others' then 0
      else hotel_cancellation_fee end as hotel_cancellation_fee
    , case when product_category = 'Others' then 0
      else si_amount end as si_amount
    , refund_fee
    , admin_fee
    , case when product_category = 'Others' then 0
      else commission end as commission
    , partner_commission
    , case when product_category = 'Others' then 0
      else subsidy end as subsidy
    , case when product_category = 'Others' then null
      else subsidy_category end as subsidy_category
    , case when product_category = 'Others' then 0
      else upselling end as upselling
    , case when product_category = 'Others' then 0
      else giftvoucher_value end as giftvoucher_value
    , case when product_category = 'Others' then 0
      else tiketpoint_value end as tiketpoint_value
    , case when product_category = 'Others' then 0
      else promocode_value end as promocode_value
    , insurance_value
    , case when product_category = 'Others' then 0
      else payment_charge end as payment_charge
    , case when product_category = 'Others' then 0
      else payment_amount end as payment_amount
    , case when product_category = 'Others' then 0
      else total_ancillary_flight end as total_ancillary_flight
    , order_payment_type
    , is_deposit_flag
    , order_detail_name
    , memo
    , case when product_category = 'Others'
        then if(customer_reference_id is null or customer_reference_id = '', 'C-000001', customer_reference_id)
      else customer_reference_id end as customer_reference_id
    , additional_info_json
    , is_issued_flag
    , is_topup_match
    , refund_status
  from
    fact10
)
, fact12 as ( /* Set up order only refund amount with admin fee/refund fee */
  select
    * except
        (
          refund_flight_amount
          , estimated_refund_flight
          , si_amount
          , commission
          , subsidy
          , subsidy_category
          , upselling
          , giftvoucher_value
          , tiketpoint_value
          , promocode_value
          , payment_charge
          , admin_fee
          , total_breakdown_amount
          , diff_refund
        )
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule
          )
        then if(product_category = 'Flight', refund_amount+coalesce(-1*admin_fee,0)+coalesce(-1*refund_fee,0), 0)
      else refund_flight_amount end as refund_flight_amount
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule
          )
        then if(product_category <> 'Flight', refund_amount + if(diff_refund <> 0 and is_reschedule and product_category in ('Hotel','Hotel_NHA') and admin_fee <> 0, -1*admin_fee, 0), 0)
      else si_amount end as si_amount  
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule 
          )
        then 0
      else estimated_refund_flight end as estimated_refund_flight
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule 
          )
        then 0
      else commission end as commission
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule 
          )
        then 0
      else subsidy end as subsidy
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule 
          )
        then null
      else subsidy_category end as subsidy_category
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule 
          )
        then 0
      else upselling end as upselling
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule 
          )
        then 0
      else giftvoucher_value end as giftvoucher_value
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule 
          )
        then 0
      else tiketpoint_value end as tiketpoint_value
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule 
          )
        then 0
      else promocode_value end as promocode_value
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule 
          )
        then 0
      else payment_charge end as payment_charge
    , case when diff_refund <> 0 and is_reschedule and product_category in ('Hotel','Hotel_NHA') and admin_fee <> 0
        then admin_fee
      when diff_refund <> 0 and is_reschedule then 0
      else admin_fee end as admin_fee
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule 
          )
        then refund_amount + if(not is_reschedule, admin_fee, 0) + refund_fee
      else total_breakdown_amount end as total_breakdown_amount
    , case when diff_refund <> 0
        and
          (
            product_category = 'Flight' and refund_flight_amount is null
            or
            order_payment_type = 'affiliate_deposit'
            or
            refund_amount = refund_flight_amount
            or
            is_reschedule 
          )
        then 0
      else diff_refund end as diff_refund
  from
    fact11
)
, fact13 as ( /* Remove ancillary flight */
  select
    * except
        (
          refund_flight_amount, total_breakdown_amount, diff_refund
        )
    , case when diff_refund <> 0 and diff_refund + total_ancillary_flight = 0
        then refund_flight_amount - total_ancillary_flight
      else refund_flight_amount end as refund_flight_amount
    , case when diff_refund <> 0 and diff_refund + total_ancillary_flight = 0
        then diff_refund + total_ancillary_flight
      else diff_refund end as diff_refund
    , case when diff_refund <> 0 and diff_refund + total_ancillary_flight = 0
        then total_breakdown_amount - total_ancillary_flight
      else total_breakdown_amount end as total_breakdown_amount
  from
    fact12
)
, fact14 as (
  select
    * except
        (
          total_breakdown_amount, diff_refund
        )
    , case when diff_refund <> 0 and payment_charge <> 0
        then total_breakdown_amount + payment_charge
      else total_breakdown_amount end as total_breakdown_amount
    , case when diff_refund <> 0 and payment_charge <> 0
        then refund_amount - (total_breakdown_amount + payment_charge)
      else diff_refund end as diff_refund
  from
    fact13
)
, fact as (
  select
    Company
    , 'IDR' as invoice_currency
    , safe_cast(1 as float64) as currency_conversion
    , supplier_reference_id
    , invoice_date
    , invoice_year
    , refund_request_date
    , refunded_date
    , due_date
    , is_partially_refund
    , refund_id
    , refund_type
    , refund_reason
    , order_id
    , order_detail_id
    , refund_pax
    , sales_pax
    , booking_code
    , refund_split_code
    , is_sabre
    , is_reschedule
    , product_category
    , product_provider
    , refund_amount
    , total_breakdown_amount
    , diff_refund
    , refund_flight_amount
    , estimated_refund_flight
    , is_hotel_paid
    , hotel_cancellation_fee
    , si_amount
    , refund_fee
    , admin_fee
    , commission
    , partner_commission
    , subsidy
    , subsidy_category
    , upselling
    , giftvoucher_value
    , tiketpoint_value
    , promocode_value
    , insurance_value
    , payment_charge
    , payment_amount
    , order_payment_type
    , is_deposit_flag
    , order_detail_name
    , memo
    , customer_reference_id
    , additional_info_json
    , is_issued_flag
    , is_topup_match
    , case when invoice_date is null and invoice_year < 2020 then false
      else
      if
        (
          diff_refund between -5000 and 5000
          , true
          , false
        )
      end as is_refund_valid_flag
    , refund_status
    , timestamp(datetime(current_timestamp(),'Asia/Jakarta')) as processed_timestamp
  from
    fact14
)
select
  *
from
  fact