with
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -1 day) as filter2
    , timestamp_add(filter1, interval 3 day) as filter3
  from
  (
    select
       timestamp_add(timestamp(date(current_timestamp(),'Asia/Jakarta')), interval -79 hour) as filter1
  )
)
, fact_product as (
  select * from `datamart-finance.datasource_workday.temp_supplier_invoice_raw_part_1`
)
, fact_vertical as (
  select
    Company
    , invoice_currency
    , supplier_reference_id
    , invoice_date
    , order_id
    , order_detail_id
    , case 
        when invoice_date > schedule_date then invoice_date
        else schedule_date
      end as due_date
    , order_detail_name
    , spend_category
    , coalesce(quantity,1) as quantity
    , total_line_amount
    , currency_conversion
    , booking_code
    , product_category
    , product_provider
    , deposit_flag
    , event_name
    , null as payment_handling
    , case 
        when deposit_flag = 'Non deposit' then 'Yes'
        else 'No'
      end as on_hold_status
    , memo
    , customer_reference_id
  from
    fact_product
)
, fact_flight_addons as ( /* EDP 16 NOV 2021: Extract addons flight / rapid test new data*/
  select
    Company
    , invoice_currency
    , supplier_reference_id
    , invoice_date
    , order_id
    , order_detail_id
    , due_date
    , order_detail_name
    , spend_category
    , sum(quantity) as quantity
    , sum(total_line_amount) as total_line_amount
    , currency_conversion
    , booking_code
    , product_category
    , product_provider
    , deposit_flag
    , event_name
    , payment_handling
    , on_hold_status
    , memo
    , customer_reference_id
  from
  (
  select
    Company
    , invoice_currency
    , case   
        when lower(json_extract_scalar(hj,'$.vendor')) = 'siloam' then '34276792'
        when lower(json_extract_scalar(hj,'$.desc')) like '%bumame%' then '34423442'
        when lower(json_extract_scalar(hj,'$.desc')) like '%halodoc%' then 'VR-00015459'
        when lower(json_extract_scalar(hj,'$.desc')) like '%farma%' then '34305569'
        else supplier_reference_id
      end as supplier_reference_id
    , invoice_date
    , order_id
    , order_detail_id
    , case 
        when invoice_date > schedule_date then invoice_date
        else schedule_date
      end as due_date
    , order_detail_name
    , case
        when lower(json_extract_scalar(hj,'$.vendor')) = 'lion' then 'Tes_Covid'
        else 'Rapid_Test'
      end as spend_category
    , halodoc_pax_count as quantity
    , safe_cast(json_extract_scalar(hj,'$.value') as float64) as total_line_amount
    , currency_conversion
    , booking_code
    , product_category
    , case
        when lower(json_extract_scalar(hj,'$.vendor')) = 'siloam' then 'Siloam'
        when lower(json_extract_scalar(hj,'$.desc')) like '%bumame%' then 'Bumame_Farmasi'
        when lower(json_extract_scalar(hj,'$.desc')) like '%halodoc%' then 'Halodoc'
        when lower(json_extract_scalar(hj,'$.desc')) like '%farma%' then 'Kimia_Farma'
        else product_provider
      end as product_provider
    , deposit_flag
    , event_name
    , null as payment_handling
    , case 
        when deposit_flag = 'Non deposit' then 'Yes'
        else 'No'
      end as on_hold_status
    , memo
    , customer_reference_id
  from
    fact_product
  cross join 
       unnest(json_extract_array(addons_flight_json)) as hj
  where
    halodoc_sell_price_amount > 0
    and lower(json_extract_scalar(hj,'$.vendor')) = 'lion' /* breakdown AP rapidtest for vendor lion only*/
  )
  group by 1,2,3,4,5,6,7,8,9,12,13,14,15,16,17,18,19,20,21 /* need group by for addons from smartroundtrip transactions - 118784547*/
)
, fact_flight_ancillary as (
  select
    Company
    , invoice_currency
    , supplier_reference_id
    , invoice_date
    , order_id
    , order_detail_id
    , case 
        when invoice_date > schedule_date then invoice_date
        else schedule_date
      end as due_date
    , order_detail_name
    , case
          when json_extract_scalar(afj,'$.category') = 'meals' then 'Meals_Flight'
          when json_extract_scalar(afj,'$.category') = 'seat_selection' then 'Seat_Flight'
          when json_extract_scalar(afj,'$.category') = 'baggage' then 'Bagage'
          else 'Ticket'
        end as spend_category
    , 1 as quantity
    , safe_cast(json_extract_scalar(afj,'$.value') as float64) as total_line_amount
    , currency_conversion
    , booking_code
    , product_category
    , product_provider
    , deposit_flag
    , event_name
    , null as payment_handling
    , case 
        when deposit_flag = 'Non deposit' then 'Yes'
        else 'No'
      end as on_hold_status
    , memo
    , customer_reference_id
  from
    fact_product
    cross join 
       unnest(json_extract_array(ancillary_flight_json)) as afj
)
, fact_add_ons_hotel as (
  select
    Company
    , invoice_currency
    , supplier_reference_id
    , invoice_date
    , order_id
    , order_detail_id
    , case 
        when invoice_date > schedule_date then invoice_date
        else schedule_date
      end as due_date
    , order_detail_name
    , add_ons_hotel.add_ons_spend_category
    , coalesce(add_ons_hotel.add_ons_hotel_quantity,1) as quantity
    , add_ons_hotel.add_ons_hotel_net_price_amount as total_line_amount
    , currency_conversion
    , booking_code
    , product_category
    , product_provider
    , deposit_flag
    , event_name
    , null as payment_handling
    , case 
        when deposit_flag = 'Non deposit' then 'Yes'
        else 'No'
      end as on_hold_status
    , memo
    , customer_reference_id
  from
    fact_product
    cross join 
       unnest(add_ons_hotel_detail_array) as add_ons_hotel
)
/* EDP 01 Des,2021: breakdown addons car data */
, fact_addons_car as (
  select
    Company
    , invoice_currency
    , supplier_reference_id
    , invoice_date
    , order_id
    , order_detail_id
    , due_date
    , order_detail_name
    , spend_category
    , sum(quantity) as quantity
    , sum(total_line_amount) as total_line_amount
    , currency_conversion
    , booking_code
    , product_category
    , product_provider
    , deposit_flag
    , event_name
    , payment_handling
    , on_hold_status
    , string_agg(distinct memo) as memo
    , customer_reference_id
  from
  (
    select
      Company
      , invoice_currency
      , supplier_reference_id
      , invoice_date
      , order_id
      , order_detail_id
      , case 
          when invoice_date > schedule_date then invoice_date
          else schedule_date
        end as due_date
      , order_detail_name
      , case
            when json_extract_scalar(acj,'$.category') = 'paid_facility' then 'Add_On_Special'
          else 'Add_On_Zone'
          end as spend_category
      , 1 as quantity
      , safe_cast(json_extract_scalar(acj,'$.value') as float64) as total_line_amount
      , currency_conversion
      , booking_code
      , product_category
      , product_provider
      , deposit_flag
      , event_name
      , null as payment_handling
      , case 
          when deposit_flag = 'Non deposit' then 'Yes'
          else 'No'
        end as on_hold_status
      , memo
      , customer_reference_id
    from
      fact_product
      cross join 
         unnest(json_extract_array(addons_car_json)) as acj
    where
      addons_car_json is not null
  )
  group by 1,2,3,4,5,6,7,8,9,12,13,14,15,16,17,18,19,21
)
, fact as (
select * from fact_vertical
union all
select * from fact_flight_ancillary
union all
select * from fact_flight_addons
union all
select * from fact_add_ons_hotel
union all
select * from fact_addons_car
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
      where date(invoice_date) >= (select date(filter1,'Asia/Jakarta') from fd)
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
    (tr.Company = fact.Company)
  and (tr.invoice_currency = fact.invoice_currency or (tr.invoice_currency is null and fact.invoice_currency is null))
  and (tr.supplier_reference_id = fact.supplier_reference_id or (tr.supplier_reference_id is null and fact.supplier_reference_id is null))
  and (tr.invoice_date = fact.invoice_date)
  and (tr.order_id = fact.order_id)
  and (tr.order_detail_id = fact.order_detail_id)
  and (tr.due_date = fact.due_date)
  and (tr.order_detail_name = fact.order_detail_name)
  and (tr.spend_category = fact.spend_category)
  and (tr.quantity = fact.quantity or (tr.quantity is null and fact.quantity is null))
  and (tr.total_line_amount = fact.total_line_amount or (tr.total_line_amount is null and fact.total_line_amount is null))
  and (tr.currency_conversion = fact.currency_conversion or (tr.currency_conversion is null and fact.currency_conversion is null))
  and (tr.booking_code = fact.booking_code or (tr.booking_code is null and fact.booking_code is null))
  and (tr.product_category = fact.product_category)
  and (tr.product_provider = fact.product_provider or (tr.product_provider is null and fact.product_provider is null))
  and (tr.deposit_flag = fact.deposit_flag or (tr.deposit_flag is null and fact.deposit_flag is null))
  and (tr.event_name = fact.event_name or (tr.event_name is null and fact.event_name is null))
  and (tr.payment_handling = fact.payment_handling or (tr.payment_handling is null and fact.payment_handling is null))
  and (tr.on_hold_status = fact.on_hold_status or (tr.on_hold_status is null and fact.on_hold_status is null))
  and (tr.memo = fact.memo or (tr.memo is null and fact.memo is null))
  and (tr.customer_reference_id = fact.customer_reference_id or (tr.customer_reference_id is null and fact.customer_reference_id is null))
where
  tr.order_detail_id is null
)
select * from append