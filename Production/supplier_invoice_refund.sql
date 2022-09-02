with
lsw as (
  select
    distinct order_id
    , order_detail_id as refund_id
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
    date(refund_request_date) = date_add(date(current_timestamp(),'Asia/Jakarta'), interval -1 day)
  qualify row_number() over(partition by order_id, order_detail_id order by processed_timestamp desc) = 1
)
, info as (
  select
    Company
    , invoice_currency
    , supplier_reference_id
    , refund_request_date
    , order_id
    , order_detail_id
    , refund_id
    , order_detail_name
    , due_date
    , currency_conversion
    , memo as booking_code
    , product_category
    , product_provider
    , 'Non deposit' as deposit_flag
    , 'Yes' as on_hold_status
    , memo
    , refund_split_code
    , refund_reason
    , customer_reference_id
    , case when is_partially_refund then refund_pax
      else sales_pax end as quantity
    , [
        struct
        (
          case
            when is_reschedule then 'Cashback'
            when refund_reason = 'Customer Double Paid' then 'Refund_Others'
            when product_category = 'Others' then 'Refund_Others'
            when product_category = 'Hotel'
              then
                if
                  (
                    is_deposit_flag
                    , 'Deposit_Hotel'
                    , if
                        (
                          is_hotel_paid
                          , 'Deposit_Hotel_Refund'
                          , 'Refund_Hotel'
                        )
                  )
            when product_category = 'Hotel_NHA'
              then
                if
                  (
                    is_deposit_flag
                    , 'Deposit_Hotel'
                    , if
                        (
                          is_hotel_paid
                          , 'Deposit_Hotel_Refund'
                          , 'Refund_Hotel_NHA'
                        )
                  )
            when product_category = 'Flight'
              then if(is_sabre, 'Refund_International_Flight', 'Refund_Flight')
            when product_category = 'Attraction'
              then if(is_deposit_flag, 'Deposit_Attraction', 'Refund_Entertainment')
            when product_category = 'Activity'
              then if(is_deposit_flag, 'Deposit_Activity', 'Refund_Entertainment')
            when product_category = 'Event'
              then if(is_deposit_flag, 'Deposit_Event', 'Refund_Entertainment')
            when product_category = 'Car'
              then if(is_deposit_flag, 'Deposit_Car','Refund_Car')
            when product_category = 'Train' then 'Refund_Train'
            else ''
          end as spend_category
          , case
              when product_category = 'Others' then refund_amount
              when product_category = 'Flight' then coalesce(refund_flight_amount,estimated_refund_flight,0)
              else ifnull(si_amount,0)
            end as total_line_amount
          , true as valid_struct_flag
          , 1 as data_order
        )
        , struct
          (
            case
              when product_category = 'Others' then 'Retur_Others'
              when product_category = 'Hotel' then 'Retur_Hotel'
              when product_category = 'Hotel_NHA' then 'Retur_Hotel_NHA'
              when product_category = 'Flight' then 'Retur_Flight'
              when product_category in ('Attraction','Activity','Event') then 'Retur_TTD'
              when product_category = 'Car' then 'Retur_Car'
              when product_category = 'Train' then 'Retur_Train'
              else ''
            end as spend_category
            , commission as total_line_amount
            , case when commission <> 0 then true
              else false end as valid_struct_flag
            , 2 as data_order
          )
        , struct
          (
            case
              when subsidy_category is not null then subsidy_category
              else 'Subsidy'
            end as spend_category
            , subsidy as total_line_amount
            , case when subsidy <> 0 then true
              else false end as valid_struct_flag
            , 3 as data_order
          )
        , struct
          (
            'Subsidy' as spend_category
            , partner_commission as total_line_amount
            , case when partner_commission <> 0 then true
              else false end as valid_struct_flag
            , 4 as data_order
          )
        , struct
          (
            case
              when product_category = 'Hotel' then 'Retur_Upselling_Hotel'
              when product_category = 'Hotel_NHA' then 'Retur_Upselling_Hotel_NHA'
              when product_category = 'Flight' then 'Retur_Upselling_Flight'
              when product_category in ('Attraction','Activity','Event') then 'Retur_Upselling_TTD'
              else 'Upselling' end as spend_category
            , upselling as total_line_amount
            , case when upselling <> 0 then true
              else false end as valid_struct_flag
            , 5 as data_order
          )
        , struct
          (
            'Refund_Fee' as spend_category
            , refund_fee as total_line_amount
            , case when refund_fee <> 0 then true
              else false end as valid_struct_flag
            , 6 as data_order
          )
        , struct
          (
            'Admin_Fee' as spend_category
            , admin_fee as total_line_amount
            , case when admin_fee <> 0 then true
              else false end as valid_struct_flag
            , 7 as data_order
          )
        , struct
          (
            'Tix_Point' as spend_category
            , tiketpoint_value as total_line_amount
            , case when tiketpoint_value <> 0 then true
              else false end as valid_struct_flag
            , 8 as data_order
          )
        , struct
          (
            'Promocode' as spend_category
            , promocode_value as total_line_amount
            , case when promocode_value <> 0 then true
              else false end as valid_struct_flag
            , 9 as data_order
          )
        , struct
          (
            'Gift_Voucher' as spend_category
            , giftvoucher_value as total_line_amount
            , case when giftvoucher_value <> 0 then true
              else false end as valid_struct_flag
            , 10 as data_order
          )
        , struct
          (
            'Bank_charges' as spend_category
            , payment_charge as total_line_amount
            , case when payment_charge <> 0 then true
              else false end as valid_struct_flag
            , 11 as data_order
          )
        , struct
          (
            'Beban_Refund' as spend_category
            , diff_refund as total_line_amount
            , case when diff_refund <> 0 then true
              else false end as valid_struct_flag
            , 12 as data_order
          )
      ] as info_array
  from
    tr
    left join lsw using (order_id, refund_id)
  where
    is_refund_valid_flag
    and is_sent_flag is null
    and supplier_reference_id <> ''
    and product_provider <> ''
)
, refund as (
  select
     coalesce(Concat('"',Company,'"'), '""') as Company
    , coalesce(Concat('"',invoice_currency,'"'), '""') as invoice_currency
    , coalesce(Concat('"',supplier_reference_id,'"'), '""') as supplier_reference_id
    , coalesce(Concat('"',safe_cast(refund_request_date as string),'"'), '""') as invoice_date
    , coalesce(Concat('"',safe_cast(order_id as string),'_RFD"'), '""') as order_id
    , coalesce(Concat('"',safe_cast(refund_id as string),'"'), '""') as order_detail_id
    , coalesce(Concat('"',safe_cast(due_date as string),'"'), '""') as due_date
    , coalesce(Concat('"',
        if(order_detail_name = '' or order_detail_name is null, refund_reason, order_detail_name),'"')
        , '""')
      as order_detail_name
    , coalesce(Concat('"',spend_category,'"'), '""') as spend_category
    , coalesce(Concat('"',safe_cast(quantity as string),'"'), '""') as quantity
    , coalesce(Concat('"',safe_cast(info_array.total_line_amount as string),'"'), '""') as total_line_amount
    , coalesce(Concat('"',
        case
          when Company <> 'GTN_IDN' then ''
          else safe_cast(round(currency_conversion,2) as string)
        end
      ,'"'), '""') as currency_conversion
    , coalesce(Concat('"',if(booking_code <> '', booking_code, 'No booking code'),' - Refund"'), '""') as booking_code
    , coalesce(Concat('"',product_category,'"'), '""') as product_category
    , coalesce(Concat('"',product_provider,'"'), '""') as product_provider
    , coalesce(Concat('"',deposit_flag,'"'), '""') as deposit_flag
    , coalesce(Concat('"',
        case
          when product_category in ('Attraction','Activity','Event') then order_detail_name
        end
      ,'"'), '""') as event_name
    , '""' as payment_handling
    , coalesce(concat('"',on_hold_status,'"'),'""') as on_hold_status
    , coalesce(concat('"'
        , if(memo = '' or memo is null, safe_cast(order_id as string), memo)
        , if(memo <> '', ' - Refund', '')
        , if(
            refund_split_code is not null and refund_split_code <> '' and refund_split_code <> '0'
            , concat(' - Splitcode: ', refund_split_code)
            , '')
        , '"'),'""') as memo
    , coalesce(concat('"',customer_reference_id,'"'),'""') as customer_reference_id
    , row_number() over (order by refund_request_date, order_id, order_detail_id, info_array.data_order) as rn
  from
    info
    cross join unnest (info.info_array) as info_array
  where info_array.valid_struct_flag
)
select
  * except(rn)
from
  refund
order by rn