with
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -1 day) as filter2
     ,timestamp_add(filter1, interval 3 day) as filter3 
  from
  (
    select
     timestamp_add(timestamp(date(current_timestamp(), 'Asia/Jakarta')), interval -79 hour) as filter1
  )
)
, oc as (
  select
    order_id
    , datetime(payment_timestamp, 'Asia/Jakarta') as payment_datetime
  from
    `datamart-finance.staging.v_order__cart`
  where
    payment_status = 'paid'
    and payment_timestamp >= (select filter1 from fd)
    and payment_timestamp < (select filter3 from fd) 
  group by
    order_id
    , payment_timestamp
)
, ocd as (
  select
    order_id
    , order_detail_id
    , order_type
    , order_name_detail
  from
    `datamart-finance.staging.v_order__cart_detail`
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp < (select filter3 from fd)
    and order_type in ('event','car','tixhotel')
    and order_detail_status in ('active','refund','refunded','hide_by_cust')
  group by
    1,2,3,4
)
, decm as (
  select
    detail_id as detail_event_id
    , string_agg(distinct supplier_id) as supplier_id
    , string_agg(distinct supplier_name) as supplier_name
    , string_agg(distinct event_name) as event_name /*Update by Rizki Habibie @2020, 18th of August*/
    , string_agg(distinct event_type) as event_type
    , string_agg(distinct ext_source) as ext_source_event
    , string_agg(distinct event_category) as event_category
  from
    `datamart-finance.staging.v_detail__event_connect_ms` 
  group by
    1
)
, oecm as (
  select
    order_id
    , order_detail_id
    , detail_event_id
    , supplier_id
    , case 
        when event_category = 'HOTEL' and lower(supplier_name) not like 'ttd %' then concat('TTD ', supplier_name)
        when lower(event_name) like ('sewa mobil%') and lower(supplier_name) not like 'ttd %' then concat('TTD ', supplier_name)
        else supplier_name
      end as supplier_name
    , ext_source_event
    , case
        when event_category = 'HOTEL' then 'Hotel'
        when lower(event_name) LIKE'%sewa mobil%' AND event_category='TRANSPORT' then 'Car' /*Update by Rizki Habibie @2020, 18th of August*/
        when event_type in ('D') then 'Attraction'
        when event_type in ('E') then 'Activity'
        when event_type not in ('D','E') then 'Event'
      end as product_category
  from
    `datamart-finance.staging.v_order__event_connect_ms`
    left join decm using (detail_event_id)
  group by
    1,2,3,4,5,6,7
)
, occar as (
  select
    distinct
    order_detail_id
    , replace(split(split(log_data,'business_id":')[safe_offset(1)],',"')[safe_offset(0)],'"','') as supplier_id
    , replace(split(split(log_data,'business_name":')[safe_offset(1)],',"')[safe_offset(0)],'"','') as supplier_name
    , 'Car' as product_category
  from
    `datamart-finance.staging.v_order__cart_car`
  where
    lastupdate >= (select filter2 from fd)
    and lastupdate < (select filter3 from fd)
)
, hb as (
  select
    safe_cast(id as string) as hotel_itinerarynumber
    , hotel_id as hotel_id_hb
  from
    `datamart-finance.staging.v_hotel_bookings`
)
, hcc as (
  select
    distinct
    _id as city_id
    , string_agg(distinct coalesce(cityName_name, cityName_nameAlias)) as city_name
  from
    `datamart-finance.staging.v_hotel_core_city_flat` 
  where
    cityName_lang = 'en'
    and name_lang = 'en'
  group by
    _id
)
, hcr as (
  select
    distinct
    _id as region_id
    , string_agg(distinct coalesce(regionName_name, name_name)) as region_name
    , case
        when _id = '5b73b927e1b2236ea6462405' then 'IDN-AC'
        when _id = '5b73b927e1b2236ea6462406' then 'IDN-BA'
        when _id = '5b73b928e1b2236ea6462415' then 'IDN-BB'
        when _id = '5b73b927e1b2236ea6462407' then 'IDN-BT'
        when _id = '5b73b927e1b2236ea6462408' then 'IDN-BE'
        when _id = '5b73b927e1b2236ea646240e' then 'IDN-JT'
        when _id = '5b73b927e1b2236ea6462412' then 'IDN-KT'
        when _id = '5b73b928e1b2236ea6462421' then 'IDN-ST'
        when _id = '5b73b927e1b2236ea646240f' then 'IDN-JI'
        when _id = '5b73b927e1b2236ea6462413' then 'IDN-KI'
        when _id = '5b73b928e1b2236ea646241b' then 'IDN-NT'
        when _id = '5b73b927e1b2236ea646240b' then 'IDN-GO'
        when _id = '5b73b927e1b2236ea646240a' then 'IDN-JK'
        when _id = '5b73b927e1b2236ea646240c' then 'IDN-JA'
        when _id = '5b73b928e1b2236ea6462417' then 'IDN-LA'
        when _id = '5b73b928e1b2236ea6462418' then 'IDN-ML'
        when _id = '5b73b928e1b2236ea6462414' then 'IDN-KU'
        when _id = '5b73b928e1b2236ea6462419' then 'IDN-MU'
        when _id = '5b73b928e1b2236ea6462423' then 'IDN-SA'
        when _id = '5b73b928e1b2236ea6462426' then 'IDN-SA'
        when _id = '5b73b928e1b2236ea646241c' then 'IDN-PP'
        when _id = '5b73b928e1b2236ea646241e' then 'IDN-RI'
        when _id = '5b73b928e1b2236ea6462416' then 'IDN-RI'
        when _id = '5b73b928e1b2236ea6462422' then 'IDN-SN'
        when _id = '5b73b927e1b2236ea6462411' then 'IDN-KS'
        when _id = '5b73b928e1b2236ea6462420' then 'IDN-SN'
        when _id = '5b73b928e1b2236ea6462425' then 'IDN-SS'
        when _id = '5b73b927e1b2236ea646240d' then 'IDN-JB'
        when _id = '5b73b927e1b2236ea6462410' then 'IDN-KB'
        when _id = '5b73b928e1b2236ea646241a' then 'IDN-NB'
        when _id = '5b73b928e1b2236ea646241d' then 'IDN-PB'
        when _id = '5b73b928e1b2236ea646241f' then 'IDN-SR'
        when _id = '5b73b928e1b2236ea6462424' then 'IDN-SB'
        when _id = '5b73b927e1b2236ea6462409' then 'IDN-YO'
        when _id = '5b73b92fe1b2236ea6462475' then 'MYS-1'
        when _id = '5b73b92fe1b2236ea6462476' then 'MYS-2'
        when _id = '5b73b92fe1b2236ea6462477' then 'MYS-3'
        when _id = '5b73b92fe1b2236ea6462478' then 'MYS-14'
        when _id = '5b73b92fe1b2236ea6462479' then 'MYS-15'
        when _id = '5b73b92fe1b2236ea646247a' then 'MYS-4'
        when _id = '5b73b92fe1b2236ea646247b' then 'MYS-5'
        when _id = '5b73b92fe1b2236ea646247c' then 'MYS-6'
        when _id = '5b73b930e1b2236ea646247f' then 'MYS-7'
        when _id = '5b73b930e1b2236ea646247d' then 'MYS-8'
        when _id = '5b73b930e1b2236ea646247e' then 'MYS-9'
        when _id = '5b73b931e1b2236ea6462481' then 'MYS-16'
        when _id = '5b73b930e1b2236ea6462480' then 'MYS-12'
        when _id = '5b73b931e1b2236ea6462482' then 'MYS-13'
        when _id = '5b73b931e1b2236ea6462483' then 'MYS-10'
        when _id = '5b73b931e1b2236ea6462484' then 'MYS-11'
        when _id = '5b73ba8ae1b2236ea647400d' then 'VNM-44'
        when _id = '5b73ba8be1b2236ea647401b' then 'VNM-30'
        when _id = '5b73ba8ce1b2236ea6474029' then 'VNM-28'
        when _id = '5b73ba8ce1b2236ea647402e' then 'VNM-41'
        when _id = '5b73bc2ce1b2236ea647462a' then 'PHL-ABR'
        when _id = '5b73bc2ce1b2236ea647462b' then 'PHL-AGN'
        when _id = '5b73bc2ce1b2236ea647462c' then 'PHL-AGS'
        when _id = '5b73bc2ce1b2236ea647462d' then 'PHL-AKL'
        when _id = '5b73bc2ce1b2236ea647462e' then 'PHL-ALB'
        when _id = '5b73bc2ce1b2236ea647462f' then 'PHL-ANT'
        when _id = '5b73bc2ce1b2236ea6474631' then 'PHL-AUR'
        when _id = '5b73bc2ce1b2236ea6474633' then 'PHL-BAN'
        when _id = '5b73bc2ce1b2236ea6474634' then 'PHL-BTN'
        when _id = '5b73bc2ce1b2236ea6474635' then 'PHL-BTG'
        when _id = '5b73bc2ce1b2236ea6474636' then 'PHL-BEN'
        when _id = '5b73bc2de1b2236ea6474637' then 'PHL-BIL'
        when _id = '5b73bc2de1b2236ea6474638' then 'PHL-BOH'
        when _id = '5b73bc2de1b2236ea6474639' then 'PHL-BUK'
        when _id = '5b73bc2de1b2236ea647463a' then 'PHL-BUL'
        when _id = '5b73bc2de1b2236ea647463b' then 'PHL-CAG'
        when _id = '5b73bc2de1b2236ea647463c' then 'PHL-CAN'
        when _id = '5b73bc2de1b2236ea647463d' then 'PHL-CAS'
        when _id = '5b73bc2de1b2236ea647463e' then 'PHL-CAM'
        when _id = '5b73bc2de1b2236ea647463f' then 'PHL-CAP'
        when _id = '5b73bc2de1b2236ea6474640' then 'PHL-CAT'
        when _id = '5b73bc2de1b2236ea6474641' then 'PHL-CAV'
        when _id = '5b73bc2de1b2236ea6474642' then 'PHL-CEB'
        when _id = '5b73bc2de1b2236ea6474643' then 'PHL-COM'
        when _id = '5b73bc2ee1b2236ea6474646' then 'PHL-DAO'
        when _id = '5b73bc2ee1b2236ea6474644' then 'PHL-DAV'
        when _id = '5b73bc2ee1b2236ea6474645' then 'PHL-DAS'
        when _id = '5b73bc2ee1b2236ea6474647' then 'PHL-DIN'
        when _id = '5b73bc2ee1b2236ea6474648' then 'PHL-EAS'
        when _id = '5b73bc2ee1b2236ea6474649' then 'PHL-GUI'
        when _id = '5b73bc2ee1b2236ea647464a' then 'PHL-IFU'
        when _id = '5b73bc2ee1b2236ea647464b' then 'PHL-ILN'
        when _id = '5b73bc2ee1b2236ea647464c' then 'PHL-ILS'
        when _id = '5b73bc2ee1b2236ea647464d' then 'PHL-ILI'
        when _id = '5b73bc2ee1b2236ea647464f' then 'PHL-KAL'
        when _id = '5b73bc2ee1b2236ea6474650' then 'PHL-LUN'
        when _id = '5b73bc2ee1b2236ea6474651' then 'PHL-LAG'
        when _id = '5b73bc2ee1b2236ea6474652' then 'PHL-LAN'
        when _id = '5b73bc2ee1b2236ea6474654' then 'PHL-LEY'
        when _id = '5b73bc2fe1b2236ea6474655' then 'PHL-MAG'
        when _id = '5b73bc2fe1b2236ea6474656' then 'PHL-MAD'
        when _id = '5b73bc2fe1b2236ea6474657' then 'PHL-MAS'
        when _id = '5b73bc2fe1b2236ea6474659' then 'PHL-MSC'
        when _id = '5b73bc2fe1b2236ea647465a' then 'PHL-MSR'
        when _id = '5b73bc2fe1b2236ea647465b' then 'PHL-MOU'
        when _id = '5b73bc2fe1b2236ea647465c' then 'PHL-NEC'
        when _id = '5b73bc2fe1b2236ea647465d' then 'PHL-NER'
        when _id = '5b73bc2fe1b2236ea647465f' then 'PHL-NSA'
        when _id = '5b73bc2fe1b2236ea6474660' then 'PHL-NUE'
        when _id = '5b73bc2fe1b2236ea6474661' then 'PHL-NUV'
        when _id = '5b73bc30e1b2236ea6474664' then 'PHL-PLW'
        when _id = '5b73bc31e1b2236ea6474665' then 'PHL-PAM'
        when _id = '5b73bc31e1b2236ea6474666' then 'PHL-PAN'
        when _id = '5b73bc31e1b2236ea6474667' then 'PHL-QUE'
        when _id = '5b73bc32e1b2236ea6474669' then 'PHL-RIZ'
        when _id = '5b73bc32e1b2236ea647466a' then 'PHL-ROM'
        when _id = '5b73bc32e1b2236ea647466b' then 'PHL-WSA'
        when _id = '5b73bc32e1b2236ea647466c' then 'PHL-SAR'
        when _id = '5b73bc32e1b2236ea647466d' then 'PHL-SIG'
        when _id = '5b73bc32e1b2236ea647466e' then 'PHL-SOR'
        when _id = '5b73bc32e1b2236ea647466f' then 'PHL-SCO'
        when _id = '5b73bc32e1b2236ea6474670' then 'PHL-SLE'
        when _id = '5b73bc32e1b2236ea6474671' then 'PHL-SUK'
        when _id = '5b73bc32e1b2236ea6474673' then 'PHL-SUN'
        when _id = '5b73bc32e1b2236ea6474674' then 'PHL-SUR'
        when _id = '5b73bc33e1b2236ea6474675' then 'PHL-TAR'
        when _id = '5b73bc33e1b2236ea6474677' then 'PHL-ZMB'
        when _id = '5b73bc33e1b2236ea647467a' then 'PHL-ZSI'
        when _id = '5b73bc33e1b2236ea6474678' then 'PHL-ZAN'
        when _id = '5b73bc33e1b2236ea6474679' then 'PHL-ZAS'
        when _id = '5b73b928e1b2236ea6462428' then 'THA-37'
        when _id = '5b73b928e1b2236ea6462429' then 'THA-15'
        when _id = '5b73b928e1b2236ea646242b' then 'THA-38'
        when _id = '5b73b928e1b2236ea646242c' then 'THA-31'
        when _id = '5b73b928e1b2236ea646242d' then 'THA-24'
        when _id = '5b73b929e1b2236ea646242e' then 'THA-18'
        when _id = '5b73b929e1b2236ea646242f' then 'THA-36'
        when _id = '5b73b929e1b2236ea6462430' then 'THA-22'
        when _id = '5b73b929e1b2236ea6462431' then 'THA-50'
        when _id = '5b73b929e1b2236ea6462432' then 'THA-57'
        when _id = '5b73b929e1b2236ea6462433' then 'THA-20'
        when _id = '5b73b929e1b2236ea6462434' then 'THA-86'
        when _id = '5b73b929e1b2236ea6462435' then 'THA-46'
        when _id = '5b73b929e1b2236ea6462436' then 'THA-62'
        when _id = '5b73b929e1b2236ea6462437' then 'THA-71'
        when _id = '5b73b929e1b2236ea6462438' then 'THA-40'
        when _id = '5b73b92ae1b2236ea6462439' then 'THA-81'
        when _id = '5b73b92ae1b2236ea646243a' then 'THA-52'
        when _id = '5b73b92ae1b2236ea646243b' then 'THA-51'
        when _id = '5b73b92ae1b2236ea646243c' then 'THA-42'
        when _id = '5b73b92ae1b2236ea646243d' then 'THA-16'
        when _id = '5b73b92ae1b2236ea646243e' then 'THA-58'
        when _id = '5b73b92ae1b2236ea646243f' then 'THA-44'
        when _id = '5b73b92ae1b2236ea6462440' then 'THA-49'
        when _id = '5b73b92ae1b2236ea6462441' then 'THA-26'
        when _id = '5b73b92ae1b2236ea6462442' then 'THA-73'
        when _id = '5b73b92ae1b2236ea6462443' then 'THA-48'
        when _id = '5b73b92ae1b2236ea6462444' then 'THA-30'
        when _id = '5b73b92be1b2236ea6462445' then 'THA-60'
        when _id = '5b73b92be1b2236ea6462446' then 'THA-80'
        when _id = '5b73b92be1b2236ea6462447' then 'THA-55'
        when _id = '5b73b92be1b2236ea6462448' then 'THA-96'
        when _id = '5b73b92be1b2236ea6462449' then 'THA-39'
        when _id = '5b73b92be1b2236ea646244a' then 'THA-43'
        when _id = '5b73b92de1b2236ea6462458' then 'THA-12'
        when _id = '5b73b92de1b2236ea6462459' then 'THA-13'
        when _id = '5b73b92de1b2236ea646245a' then 'THA-94'
        when _id = '5b73b92de1b2236ea646245b' then 'THA-82'
        when _id = '5b73b92de1b2236ea646245c' then 'THA-93'
        when _id = '5b73b92de1b2236ea646245d' then 'THA-56'
        when _id = '5b73b92de1b2236ea646245e' then 'THA-67'
        when _id = '5b73b92de1b2236ea646245f' then 'THA-76'
        when _id = '5b73b92de1b2236ea6462460' then 'THA-66'
        when _id = '5b73b92de1b2236ea6462461' then 'THA-65'
        when _id = '5b73b92de1b2236ea6462462' then 'THA-14'
        when _id = '5b73b92de1b2236ea6462463' then 'THA-54'
        when _id = '5b73b92de1b2236ea6462464' then 'THA-83'
        when _id = '5b73b92ee1b2236ea6462465' then 'THA-25'
        when _id = '5b73b92ee1b2236ea6462466' then 'THA-77'
        when _id = '5b73b92ee1b2236ea6462467' then 'THA-85'
        when _id = '5b73b92ee1b2236ea6462468' then 'THA-70'
        when _id = '5b73b92ee1b2236ea6462469' then 'THA-21'
        when _id = '5b73b92ee1b2236ea646246a' then 'THA-45'
        when _id = '5b73b92ee1b2236ea646246b' then 'THA-27'
        when _id = '5b73b92ee1b2236ea646246c' then 'THA-47'
        when _id = '5b73b92ee1b2236ea646246d' then 'THA-11'
        when _id = '5b73b92ee1b2236ea646246e' then 'THA-74'
        when _id = '5b73b92ee1b2236ea646246f' then 'THA-75'
        when _id = '5b73b92ee1b2236ea6462470' then 'THA-19'
        when _id = '5b73b92ee1b2236ea6462471' then 'THA-91'
        when _id = '5b73b92fe1b2236ea6462472' then 'THA-33'
        when _id = '5b73b92fe1b2236ea6462473' then 'THA-17'
        when _id = '5b73b92fe1b2236ea6462474' then 'THA-90'
        when _id = '5b73b92be1b2236ea646244b' then 'THA-64'
        when _id = '5b73b92be1b2236ea646244c' then 'THA-72'
        when _id = '5b73b92be1b2236ea646244d' then 'THA-84'
        when _id = '5b73b92be1b2236ea646244e' then 'THA-32'
        when _id = '5b73b92ce1b2236ea646244f' then 'THA-63'
        when _id = '5b73b92ce1b2236ea6462450' then 'THA-92'
        when _id = '5b73b92ce1b2236ea6462451' then 'THA-23'
        when _id = '5b73b92ce1b2236ea6462452' then 'THA-34'
        when _id = '5b73b92ce1b2236ea6462453' then 'THA-41'
        when _id = '5b73b92ce1b2236ea6462454' then 'THA-61'
        when _id = '5b73b92ce1b2236ea6462455' then 'THA-53'
        when _id = '5b73b92de1b2236ea6462456' then 'THA-95'
        when _id = '5b73b92de1b2236ea6462457' then 'THA-35'
        when _id = '5b73bc90e1b2236ea6474aba' then 'CHN-AH'
        when _id = '5b73bc90e1b2236ea6474abb' then 'CHN-BJ'
        when _id = '5b73bc90e1b2236ea6474abc' then 'CHN-CQ'
        when _id = '5b73bc91e1b2236ea6474abd' then 'CHN-FJ'
        when _id = '5b73bc92e1b2236ea6474abe' then 'CHN-GS'
        when _id = '5b73bc94e1b2236ea6474abf' then 'CHN-GD'
        when _id = '5b73bc95e1b2236ea6474ac0' then 'CHN-GX'
        when _id = '5b73bc95e1b2236ea6474ac1' then 'CHN-GZ'
        when _id = '5b73bc95e1b2236ea6474ac2' then 'CHN-HI'
        when _id = '5b73bc96e1b2236ea6474ac3' then 'CHN-HE'
        when _id = '5b73bc96e1b2236ea6474ac4' then 'CHN-HL'
        when _id = '5b73bc96e1b2236ea6474ac5' then 'CHN-HA'
        when _id = '5b73bc96e1b2236ea6474ac6' then 'CHN-HB'
        when _id = '5b73bc96e1b2236ea6474ac7' then 'CHN-HN'
        when _id = '5b73bc96e1b2236ea6474ac8' then 'CHN-JS'
        when _id = '5b73bc97e1b2236ea6474ac9' then 'CHN-JX'
        when _id = '5b73bc97e1b2236ea6474aca' then 'CHN-JL'
        when _id = '5b73bc97e1b2236ea6474acb' then 'CHN-LN'
        when _id = '5b73bc98e1b2236ea6474acc' then 'CHN-NM'
        when _id = '5b73bc98e1b2236ea6474ace' then 'CHN-QH'
        when _id = '5b73bc98e1b2236ea6474acf' then 'CHN-SN'
        when _id = '5b73bc99e1b2236ea6474ad0' then 'CHN-SD'
        when _id = '5b73bc9ae1b2236ea6474ad1' then 'CHN-SH'
        when _id = '5b73bc9ae1b2236ea6474ad2' then 'CHN-SX'
        when _id = '5b73bc9ae1b2236ea6474ad3' then 'CHN-SC'
        when _id = '5b73bc9ae1b2236ea6474ad4' then 'CHN-TJ'
        when _id = '5b73bc9ae1b2236ea6474ad6' then 'CHN-XZ'
        when _id = '5b73bc9be1b2236ea6474ad7' then 'CHN-YN'
        when _id = '5b73bc9de1b2236ea6474ad8' then 'CHN-ZJ'
        when _id = '5b73bbbee1b2236ea64743ad' then 'TLS-DI'
        when _id = '5b73bba2e1b2236ea6474386' then 'AUS-ACT'
        when _id = '5b73bba3e1b2236ea6474389' then 'AUS-NSW'
        when _id = '5b73bba7e1b2236ea647438a' then 'AUS-NT'
        when _id = '5b73bbade1b2236ea647438b' then 'AUS-QLD'
        when _id = '5b73bbafe1b2236ea647438c' then 'AUS-SA'
        when _id = '5b73bbb1e1b2236ea647438d' then 'AUS-TAS'
        when _id = '5b73bbb2e1b2236ea647438e' then 'AUS-VIC'
        when _id = '5b73bbbbe1b2236ea647438f' then 'AUS-WA'
        when _id = '5b73bc53e1b2236ea64748fe' then 'KOR-43'
        when _id = '5b73bc53e1b2236ea64748ff' then 'KOR-44'
        when _id = '5b73bc53e1b2236ea6474902' then 'KOR-42'
        when _id = '5b73bc53e1b2236ea6474904' then 'KOR-41'
        when _id = '5b73bc54e1b2236ea6474905' then 'KOR-47'
        when _id = '5b73bc54e1b2236ea6474906' then 'KOR-48'
        when _id = '5b73bc55e1b2236ea6474909' then 'KOR-45'
        when _id = '5b73bc58e1b2236ea647490a' then 'KOR-46'
        when _id = '5b73bb65e1b2236ea6474230' then 'CHN-FJ'
        when _id = '5b73bb65e1b2236ea6474231' then 'TWN-KHH'
        when _id = '5b73bb65e1b2236ea6474232' then 'TWN-NWT'
        when _id = '5b73bb65e1b2236ea6474233' then 'TWN-TXG'
        when _id = '5b73bb65e1b2236ea6474234' then 'TWN-TNN'
        when _id = '5b73bb65e1b2236ea6474235' then 'TWN-TPE'
        when _id = '5b73bb77e1b2236ea6474298' then 'BHS-CS'
        when _id = '5b73bb77e1b2236ea647429a' then 'BHS-FP'
        when _id = '5b73bb78e1b2236ea647429f' then 'BHS-HI'
        when _id = '5b73bb78e1b2236ea64742a0' then 'BHS-HT'
        when _id = '5b73bb78e1b2236ea64742a2' then 'BHS-LI'
        when _id = '5b73bb78e1b2236ea64742a6' then 'BHS-NP'
        when _id = '5b73bb79e1b2236ea64742ac' then 'SLV-SS'
        when _id = '5b73bb79e1b2236ea64742af' then 'BHS-SE'
        when _id = '5b73bb65e1b2236ea647422c' then 'BWA-CE'
        when _id = '5b73bb65e1b2236ea647422e' then 'FJI-N'
        when _id = '5b73bb65e1b2236ea647422f' then 'BWA-SO'
        when _id = '5b73bc50e1b2236ea64748cf' then 'BGD-A'
        when _id = '5b73bc51e1b2236ea64748d0' then 'BGD-B'
        when _id = '5b73bc51e1b2236ea64748d1' then 'BGD-C'
        when _id = '5b73bc51e1b2236ea64748d2' then 'BGD-D'
        when _id = '5b73bc52e1b2236ea64748d3' then 'BGD-E'
        when _id = '5b73bc52e1b2236ea64748d4' then 'BGD-F'
        when _id = '5b73bc52e1b2236ea64748d5' then 'BGD-G'
        when _id = '5b73baa9e1b2236ea64740fc' then 'BRA-AC'
        when _id = '5b73baa9e1b2236ea64740fd' then 'BRA-AL'
        when _id = '5b73baaae1b2236ea64740ff' then 'BRA-AM'
        when _id = '5b73baaae1b2236ea6474100' then 'BRA-BA'
        when _id = '5b73baabe1b2236ea6474102' then 'BRA-DF'
        when _id = '5b73baade1b2236ea6474107' then 'BRA-MT'
        when _id = '5b73baade1b2236ea6474106' then 'BRA-MS'
        when _id = '5b73baade1b2236ea6474108' then 'BRA-MG'
        when _id = '5b73baaee1b2236ea647410c' then 'BRA-PE'
        when _id = '5b73baafe1b2236ea647410f' then 'BRA-RN'
        when _id = '5b73bab0e1b2236ea6474110' then 'BRA-RS'
        when _id = '5b73baafe1b2236ea647410e' then 'BRA-RJ'
        when _id = '5b73bab0e1b2236ea6474112' then 'BRA-RR'
        when _id = '5b73bab1e1b2236ea6474114' then 'BRA-SC'
        when _id = '5b73bab1e1b2236ea6474115' then 'BRA-SE'
        when _id = '5b73bab1e1b2236ea6474116' then 'BRA-TO'
        when _id = '5b73bc06e1b2236ea64744af' then 'BRN-BE'
        when _id = '5b73bc06e1b2236ea64744b1' then 'BRN-TE'
        when _id = '5b73bc06e1b2236ea64744b2' then 'BRN-TU'
        when _id = '5b73bbf8e1b2236ea647442d' then 'KHM-24'
        when _id = '5b73bbf8e1b2236ea647442e' then 'KHM-18'
        when _id = '5b73bbf8e1b2236ea6474431' then 'KHM-12'
        when _id = '5b73babde1b2236ea647418e' then 'CAN-AB'
        when _id = '5b73bacae1b2236ea647418f' then 'CAN-BC'
        when _id = '5b73bacee1b2236ea6474191' then 'CAN-NB'
        when _id = '5b73badfe1b2236ea6474192' then 'CAN-NL'
        when _id = '5b73bae7e1b2236ea6474193' then 'CAN-NT'
        when _id = '5b73baeae1b2236ea6474194' then 'CAN-NS'
        when _id = '5b73bb40e1b2236ea6474197' then 'CAN-NU'
        when _id = '5b73bb41e1b2236ea6474198' then 'CAN-ON'
        when _id = '5b73bb42e1b2236ea6474199' then 'CAN-PE'
        when _id = '5b73bb5be1b2236ea647419b' then 'CAN-SK'
        when _id = '5b73bb5be1b2236ea647419c' then 'CAN-YT'
        when _id = '5b73baa1e1b2236ea64740c9' then 'TCD-TI'
        when _id = '5b73bb8ee1b2236ea647431c' then 'CHL-AN'
        when _id = '5b73bb8ee1b2236ea647431e' then 'CHL-AP'
        when _id = '5b73bb8ee1b2236ea647431f' then 'CHL-AT'
        when _id = '5b73bb8ee1b2236ea6474321' then 'CHL-CO'
        when _id = '5b73bb8ee1b2236ea6474322' then 'CHL-LI'
        when _id = '5b73bb8ee1b2236ea6474323' then 'CHL-LL'
        when _id = '5b73bb96e1b2236ea6474327' then 'CHL-ML'
        when _id = '5b73ba87e1b2236ea6473fe8' then 'FRA-BRE'
        when _id = '5b73ba88e1b2236ea6473fe9' then 'CMR-CE'
        when _id = '5b73ba88e1b2236ea6473feb' then 'FRA-COR'
        when _id = '5b73bc44e1b2236ea64747d9' then 'GEO-AB'
        when _id = '5b73bc44e1b2236ea64747da' then 'GEO-AJ'
        when _id = '5b73bc44e1b2236ea64747db' then 'GEO-GU'
        when _id = '5b73bc44e1b2236ea64747dc' then 'GEO-IM'
        when _id = '5b73bc44e1b2236ea64747de' then 'GEO-KK'
        when _id = '5b73bc44e1b2236ea64747df' then 'GEO-MM'
        when _id = '5b73bc44e1b2236ea64747e1' then 'GEO-SZ'
        when _id = '5b73bc44e1b2236ea64747e2' then 'GEO-SJ'
        when _id = '5b73bc44e1b2236ea64747e3' then 'GEO-SK'
        when _id = '5b73bc44e1b2236ea64747e4' then 'GEO-TB'
        when _id = '5b73bc73e1b2236ea6474986' then 'IND-AP'
        when _id = '5b73bc73e1b2236ea6474987' then 'IND-AR'
        when _id = '5b73bc73e1b2236ea6474988' then 'IND-AS'
        when _id = '5b73bc73e1b2236ea6474989' then 'IND-BR'
        when _id = '5b73bc73e1b2236ea647498a' then 'IND-CH'
        when _id = '5b73bc73e1b2236ea647498b' then 'IND-CT'
        when _id = '5b73bc73e1b2236ea647498c' then 'IND-DN'
        when _id = '5b73bc73e1b2236ea647498d' then 'IND-DD'
        when _id = '5b73bc73e1b2236ea647498e' then 'IND-GA'
        when _id = '5b73bc74e1b2236ea647498f' then 'IND-GJ'
        when _id = '5b73bc75e1b2236ea6474990' then 'IND-HR'
        when _id = '5b73bc75e1b2236ea6474991' then 'IND-HP'
        when _id = '5b73bc75e1b2236ea6474992' then 'IND-JK'
        when _id = '5b73bc75e1b2236ea6474993' then 'IND-JH'
        when _id = '5b73bc75e1b2236ea6474994' then 'IND-KA'
        when _id = '5b73bc76e1b2236ea6474995' then 'IND-KL'
        when _id = '5b73bc76e1b2236ea6474996' then 'IND-LD'
        when _id = '5b73bc76e1b2236ea6474997' then 'IND-MP'
        when _id = '5b73bc76e1b2236ea6474998' then 'IND-MH'
        when _id = '5b73bc77e1b2236ea6474999' then 'IND-MN'
        when _id = '5b73bc77e1b2236ea647499a' then 'IND-ML'
        when _id = '5b73bc77e1b2236ea647499b' then 'IND-MZ'
        when _id = '5b73bc77e1b2236ea647499c' then 'IND-NL'
        when _id = '5b73bc77e1b2236ea647499e' then 'IND-OR'
        when _id = '5b73bc77e1b2236ea647499f' then 'IND-PY'
        when _id = '5b73bc77e1b2236ea64749a0' then 'IND-PB'
        when _id = '5b73bc77e1b2236ea64749a1' then 'IND-RJ'
        when _id = '5b73bc77e1b2236ea64749a2' then 'IND-SK'
        when _id = '5b73bc77e1b2236ea64749a4' then 'IND-TN'
        when _id = '5b73bc78e1b2236ea64749a5' then 'IND-TG'
        when _id = '5b73bc78e1b2236ea64749a6' then 'IND-TR'
        when _id = '5b73bc78e1b2236ea64749a7' then 'IND-UP'
        when _id = '5b73bc78e1b2236ea64749a8' then 'IND-UT'
        when _id = '5b73bc78e1b2236ea64749a9' then 'IND-WB'
        when _id = '5b73bc47e1b2236ea647482f' then 'ITA-65'
        when _id = '5b73bc47e1b2236ea6474831' then 'ITA-77'
        when _id = '5b73bc47e1b2236ea6474832' then 'ITA-78'
        when _id = '5b73bc48e1b2236ea6474833' then 'ITA-72'
        when _id = '5b73bc48e1b2236ea6474834' then 'ITA-45'
        when _id = '5b73bc48e1b2236ea6474835' then 'ITA-36'
        when _id = '5b73bc48e1b2236ea6474836' then 'ITA-62'
        when _id = '5b73bc48e1b2236ea6474837' then 'ITA-42'
        when _id = '5b73bc48e1b2236ea6474838' then 'ITA-25'
        when _id = '5b73bc48e1b2236ea6474839' then 'ITA-57'
        when _id = '5b73bc49e1b2236ea647483a' then 'ITA-67'
        when _id = '5b73bc49e1b2236ea647483b' then 'ITA-21'
        when _id = '5b73bc49e1b2236ea647483c' then 'ITA-88'
        when _id = '5b73bc4ae1b2236ea647483e' then 'ITA-52'
        when _id = '5b73bc4ae1b2236ea647483f' then 'ITA-32'
        when _id = '5b73bc4ae1b2236ea6474840' then 'ITA-55'
        when _id = '5b73bc4ae1b2236ea6474841' then 'ITA-23'
        when _id = '5b73bc4be1b2236ea6474842' then 'ITA-34'
        when _id = '5b73bccee1b2236ea6474c84' then 'JPN-23'
        when _id = '5b73bccee1b2236ea6474c85' then 'JPN-05'
        when _id = '5b73bccee1b2236ea6474c86' then 'JPN-02'
        when _id = '5b73bccee1b2236ea6474c87' then 'JPN-12'
        when _id = '5b73bccfe1b2236ea6474c88' then 'JPN-38'
        when _id = '5b73bccfe1b2236ea6474c89' then 'JPN-18'
        when _id = '5b73bccfe1b2236ea6474c8a' then 'JPN-40'
        when _id = '5b73bccfe1b2236ea6474c8b' then 'JPN-07'
        when _id = '5b73bccfe1b2236ea6474c8c' then 'JPN-21'
        when _id = '5b73bccfe1b2236ea6474c8d' then 'JPN-10'
        when _id = '5b73bccfe1b2236ea6474c8e' then 'JPN-34'
        when _id = '5b73bcd0e1b2236ea6474c8f' then 'JPN-01'
        when _id = '5b73bcd0e1b2236ea6474c91' then 'JPN-08'
        when _id = '5b73bcd0e1b2236ea6474c92' then 'JPN-17'
        when _id = '5b73bcd0e1b2236ea6474c93' then 'JPN-03'
        when _id = '5b73bcd0e1b2236ea6474c94' then 'JPN-37'
        when _id = '5b73bcd1e1b2236ea6474c95' then 'JPN-46'
        when _id = '5b73bcd1e1b2236ea6474c96' then 'JPN-14'
        when _id = '5b73bcd1e1b2236ea6474c97' then 'JPN-39'
        when _id = '5b73bcd1e1b2236ea6474c98' then 'JPN-43'
        when _id = '5b73bcd1e1b2236ea6474c99' then 'JPN-26'
        when _id = '5b73bcd2e1b2236ea6474c9a' then 'JPN-24'
        when _id = '5b73bcd2e1b2236ea6474c9b' then 'JPN-04'
        when _id = '5b73bcd2e1b2236ea6474c9c' then 'JPN-45'
        when _id = '5b73bcd2e1b2236ea6474c9d' then 'JPN-20'
        when _id = '5b73bcd3e1b2236ea6474c9f' then 'JPN-29'
        when _id = '5b73bcd3e1b2236ea6474ca0' then 'JPN-15'
        when _id = '5b73bcd3e1b2236ea6474ca1' then 'JPN-44'
        when _id = '5b73bcd3e1b2236ea6474ca2' then 'JPN-33'
        when _id = '5b73bcd4e1b2236ea6474ca3' then 'JPN-47'
        when _id = '5b73bcd4e1b2236ea6474ca4' then 'JPN-27'
        when _id = '5b73bcd4e1b2236ea6474ca5' then 'JPN-41'
        when _id = '5b73bcd4e1b2236ea6474ca6' then 'JPN-11'
        when _id = '5b73bcd4e1b2236ea6474ca7' then 'JPN-25'
        when _id = '5b73bcd4e1b2236ea6474ca8' then 'JPN-32'
        when _id = '5b73bcd4e1b2236ea6474ca9' then 'JPN-22'
        when _id = '5b73bcd4e1b2236ea6474caa' then 'JPN-09'
        when _id = '5b73bcd4e1b2236ea6474cab' then 'JPN-36'
        when _id = '5b73bcd4e1b2236ea6474cac' then 'JPN-13'
        when _id = '5b73bcd4e1b2236ea6474cad' then 'JPN-31'
        when _id = '5b73bcd4e1b2236ea6474cae' then 'JPN-16'
        when _id = '5b73bcd4e1b2236ea6474caf' then 'JPN-30'
        when _id = '5b73bcd5e1b2236ea6474cb0' then 'JPN-06'
        when _id = '5b73bcd5e1b2236ea6474cb1' then 'JPN-35'
        when _id = '5b73bcd5e1b2236ea6474cb2' then 'JPN-19'
        when _id = '5b73bbf6e1b2236ea6474410' then 'LAO-AT'
        when _id = '5b73bbf6e1b2236ea6474413' then 'LAO-CH'
        when _id = '5b73bbf6e1b2236ea6474414' then 'LAO-HO'
        when _id = '5b73bbf6e1b2236ea6474415' then 'LAO-KH'
        when _id = '5b73bbf6e1b2236ea6474416' then 'LAO-LM'
        when _id = '5b73bc7ae1b2236ea64749c2' then 'MDG-T'
        when _id = '5b73bc7ae1b2236ea64749c3' then 'MDG-D'
        when _id = '5b73bc7be1b2236ea64749c4' then 'MDG-F'
        when _id = '5b73bc7ce1b2236ea64749c5' then 'MDG-M'
        when _id = '5b73bc7de1b2236ea64749c6' then 'MDG-A'
        when _id = '5b73bc3de1b2236ea647478a' then 'MMR-07'
        when _id = '5b73bc3de1b2236ea647478b' then 'MMR-02'
        when _id = '5b73bc3de1b2236ea647478c' then 'MMR-14'
        when _id = '5b73bc3de1b2236ea647478d' then 'MMR-11'
        when _id = '5b73bc3de1b2236ea647478e' then 'MMR-12'
        when _id = '5b73bc3de1b2236ea647478f' then 'MMR-13'
        when _id = '5b73bc3de1b2236ea6474790' then 'MMR-03'
        when _id = '5b73bc3de1b2236ea6474791' then 'MMR-04'
        when _id = '5b73bc3ee1b2236ea6474792' then 'MMR-15'
        when _id = '5b73bc3fe1b2236ea6474795' then 'MMR-01'
        when _id = '5b73bc3fe1b2236ea6474796' then 'MMR-17'
        when _id = '5b73bc41e1b2236ea6474797' then 'MMR-05'
        when _id = '5b73bc42e1b2236ea6474798' then 'MMR-06'
        when _id = '5b73bc4de1b2236ea6474859' then 'BWA-CE'
        when _id = '5b73bc80e1b2236ea64749f3' then 'NZL-AUK'
        when _id = '5b73bc81e1b2236ea64749f4' then 'NZL-BOP'
        when _id = '5b73bc81e1b2236ea64749f5' then 'NZL-CAN'
        when _id = '5b73bc81e1b2236ea64749f7' then 'NZL-GIS'
        when _id = '5b73bc81e1b2236ea64749f8' then 'NZL-HKB'
        when _id = '5b73bc81e1b2236ea64749f9' then 'NZL-MWT'
        when _id = '5b73bc82e1b2236ea64749fa' then 'NZL-MBH'
        when _id = '5b73bc82e1b2236ea64749fb' then 'NZL-NSN'
        when _id = '5b73bc82e1b2236ea64749fc' then 'NZL-NTL'
        when _id = '5b73bc83e1b2236ea64749fd' then 'NZL-OTA'
        when _id = '5b73bc83e1b2236ea64749fe' then 'NZL-STL'
        when _id = '5b73bc84e1b2236ea64749ff' then 'NZL-TKI'
        when _id = '5b73bc84e1b2236ea6474a00' then 'NZL-WKO'
        when _id = '5b73bc84e1b2236ea6474a01' then 'NZL-WGN'
        when _id = '5b73bc84e1b2236ea6474a02' then 'NZL-WTC'
        when _id = '5b73babae1b2236ea647416a' then 'OMN-BU'
        when _id = '5b73babae1b2236ea647416c' then 'OMN-WU'
        when _id = '5b73babae1b2236ea6474170' then 'OMN-MU'
        when _id = '5b73bca0e1b2236ea6474b2f' then 'PER-ANC'
        when _id = '5b73bca1e1b2236ea6474b31' then 'PER-ARE'
        when _id = '5b73bca1e1b2236ea6474b32' then 'PER-AYA'
        when _id = '5b73bca1e1b2236ea6474b33' then 'PER-CAJ'
        when _id = '5b73bca1e1b2236ea6474b35' then 'PER-CUS'
        when _id = '5b73bca1e1b2236ea6474b37' then 'PER-HUV'
        when _id = '5b73bca1e1b2236ea6474b38' then 'PER-ICA'
        when _id = '5b73bca2e1b2236ea6474b3a' then 'PER-LAL'
        when _id = '5b73bca2e1b2236ea6474b3b' then 'PER-LAM'
        when _id = '5b73bca2e1b2236ea6474b3d' then 'PER-LIM'
        when _id = '5b73bca2e1b2236ea6474b3e' then 'PER-LOR'
        when _id = '5b73bca2e1b2236ea6474b3f' then 'PER-MDD'
        when _id = '5b73bca2e1b2236ea6474b40' then 'PER-MOQ'
        when _id = '5b73bca2e1b2236ea6474b41' then 'PER-PAS'
        when _id = '5b73bca2e1b2236ea6474b42' then 'PER-PIU'
        when _id = '5b73bca2e1b2236ea6474b43' then 'PER-PUN'
        when _id = '5b73bca3e1b2236ea6474b45' then 'PER-TAC'
        when _id = '5b73bca3e1b2236ea6474b46' then 'PER-TUM'
        when _id = '5b73bca3e1b2236ea6474b47' then 'PER-UCA'
        when _id = '5b73bc67e1b2236ea647495b' then 'RUS-MOW'
        when _id = '5b73bc28e1b2236ea64745f0' then 'SAU-11'
        when _id = '5b73bc28e1b2236ea64745f1' then 'SAU-08'
        when _id = '5b73bc28e1b2236ea64745f2' then 'SAU-12'
        when _id = '5b73bc28e1b2236ea64745f5' then 'SAU-01'
        when _id = '5b73bc28e1b2236ea64745f6' then 'EGY-SHR'
        when _id = '5b73bc29e1b2236ea64745f7' then 'SAU-06'
        when _id = '5b73bc2ae1b2236ea64745fa' then 'SAU-10'
        when _id = '5b73bc2ae1b2236ea64745fb' then 'SAU-07'
        when _id = '5b73bcd8e1b2236ea6474cc5' then 'SYC-02'
        when _id = '5b73bcd8e1b2236ea6474cc4' then 'SYC-01'
        when _id = '5b73bcd8e1b2236ea6474cc8' then 'SYC-06'
        when _id = '5b73bcd8e1b2236ea6474cca' then 'SYC-08'
        when _id = '5b73bcd8e1b2236ea6474ccd' then 'SYC-11'
        when _id = '5b73bcd8e1b2236ea6474cce' then 'SYC-16'
        when _id = '5b73bcd8e1b2236ea6474cd9' then 'SYC-21'
        when _id = '5b73bcd8e1b2236ea6474cdc' then 'SYC-23'
        when _id = '5b73b928e1b2236ea6462427' then 'SGP-01'
        when _id = '5b73bab3e1b2236ea6474124' then 'ESP-CB'
        when _id = '5b73bab3e1b2236ea6474125' then 'ESP-CM'
        when _id = '5b73bab3e1b2236ea647412c' then 'ESP-EX'
        when _id = '5b73bab4e1b2236ea647412d' then 'ESP-GA'
        when _id = '5b73bab5e1b2236ea6474130' then 'ARG-F'
        when _id = '5b73bc23e1b2236ea647456a' then 'TUR-01'
        when _id = '5b73bc23e1b2236ea647456b' then 'TUR-02'
        when _id = '5b73bc23e1b2236ea647456d' then 'TUR-04'
        when _id = '5b73bc23e1b2236ea647456e' then 'TUR-68'
        when _id = '5b73bc23e1b2236ea647456f' then 'TUR-05'
        when _id = '5b73bc23e1b2236ea6474570' then 'TUR-06'
        when _id = '5b73bc23e1b2236ea6474571' then 'TUR-07'
        when _id = '5b73bc23e1b2236ea6474572' then 'TUR-75'
        when _id = '5b73bc23e1b2236ea6474573' then 'TUR-08'
        when _id = '5b73bc23e1b2236ea6474574' then 'TUR-09'
        when _id = '5b73bc23e1b2236ea6474575' then 'TUR-10'
        when _id = '5b73bc23e1b2236ea6474577' then 'TUR-72'
        when _id = '5b73bc23e1b2236ea6474578' then 'TUR-69'
        when _id = '5b73bc23e1b2236ea6474579' then 'TUR-11'
        when _id = '5b73bc23e1b2236ea647457b' then 'TUR-13'
        when _id = '5b73bc23e1b2236ea647457c' then 'TUR-14'
        when _id = '5b73bc23e1b2236ea647457d' then 'TUR-15'
        when _id = '5b73bc23e1b2236ea647457e' then 'TUR-16'
        when _id = '5b73bc23e1b2236ea6474580' then 'TUR-20'
        when _id = '5b73bc23e1b2236ea6474581' then 'TUR-21'
        when _id = '5b73bc23e1b2236ea6474582' then 'TUR-22'
        when _id = '5b73bc23e1b2236ea6474584' then 'TUR-24'
        when _id = '5b73bc24e1b2236ea6474585' then 'TUR-25'
        when _id = '5b73bc24e1b2236ea6474586' then 'TUR-26'
        when _id = '5b73bc24e1b2236ea6474588' then 'TUR-27'
        when _id = '5b73bc24e1b2236ea6474589' then 'TUR-28'
        when _id = '5b73bc24e1b2236ea647458a' then 'TUR-30'
        when _id = '5b73bc24e1b2236ea647458b' then 'TUR-31'
        when _id = '5b73bc24e1b2236ea647458d' then 'TUR-32'
        when _id = '5b73bc24e1b2236ea647458e' then 'TUR-34'
        when _id = '5b73bc24e1b2236ea647458f' then 'TUR-35'
        when _id = '5b73bc24e1b2236ea6474593' then 'TUR-70'
        when _id = '5b73bc24e1b2236ea6474594' then 'TUR-36'
        when _id = '5b73bc24e1b2236ea6474595' then 'TUR-37'
        when _id = '5b73bc24e1b2236ea6474596' then 'TUR-38'
        when _id = '5b73bc24e1b2236ea6474597' then 'TUR-79'
        when _id = '5b73bc24e1b2236ea6474599' then 'TUR-39'
        when _id = '5b73bc24e1b2236ea647459a' then 'TUR-40'
        when _id = '5b73bc24e1b2236ea647459b' then 'TUR-41'
        when _id = '5b73bc24e1b2236ea647459c' then 'TUR-42'
        when _id = '5b73bc24e1b2236ea647459d' then 'TUR-44'
        when _id = '5b73bc25e1b2236ea647459e' then 'TUR-45'
        when _id = '5b73bc25e1b2236ea647459f' then 'TUR-47'
        when _id = '5b73bc25e1b2236ea64745a0' then 'TUR-33'
        when _id = '5b73bc25e1b2236ea64745a1' then 'TUR-48'
        when _id = '5b73bc25e1b2236ea64745a2' then 'TUR-49'
        when _id = '5b73bc25e1b2236ea64745a3' then 'TUR-50'
        when _id = '5b73bc25e1b2236ea64745a4' then 'TUR-51'
        when _id = '5b73bc25e1b2236ea64745a5' then 'TUR-52'
        when _id = '5b73bc25e1b2236ea64745a6' then 'TUR-80'
        when _id = '5b73bc25e1b2236ea64745a7' then 'TUR-53'
        when _id = '5b73bc25e1b2236ea64745a8' then 'TUR-54'
        when _id = '5b73bc25e1b2236ea64745a9' then 'TUR-55'
        when _id = '5b73bc26e1b2236ea64745aa' then 'TUR-63'
        when _id = '5b73bc26e1b2236ea64745ab' then 'TUR-56'
        when _id = '5b73bc26e1b2236ea64745ac' then 'TUR-57'
        when _id = '5b73bc26e1b2236ea64745ad' then 'TUR-73'
        when _id = '5b73bc26e1b2236ea64745ae' then 'TUR-58'
        when _id = '5b73bc26e1b2236ea64745af' then 'TUR-59'
        when _id = '5b73bc26e1b2236ea64745b0' then 'TUR-60'
        when _id = '5b73bc26e1b2236ea64745b1' then 'TUR-61'
        when _id = '5b73bc26e1b2236ea64745b2' then 'TUR-62'
        when _id = '5b73bc26e1b2236ea64745b3' then 'TUR-64'
        when _id = '5b73bc26e1b2236ea64745b4' then 'TUR-65'
        when _id = '5b73bc26e1b2236ea64745b5' then 'TUR-77'
        when _id = '5b73bc26e1b2236ea64745b6' then 'TUR-66'
        when _id = '5b73bb75e1b2236ea6474289' then 'ARE-UQ'
        when _id = '5dea32be676cc16b26312b70' then 'GBR-ENG'
        when _id = '5b73bb9be1b2236ea647437f' then 'GBR-ENG'
        when _id = '5dea32bf676cc16b26312b71' then 'GBR-NIR'
        when _id = '5b73bb9be1b2236ea6474380' then 'GBR-NIR'
        when _id = '5dea32c4676cc16b26312b72' then 'GBR-SCT'
        when _id = '5b73bb9fe1b2236ea6474381' then 'GBR-SCT'
        when _id = '5dea32c7676cc16b26312b73' then 'GBR-WLS'
        when _id = '5b73bba1e1b2236ea6474382' then 'GBR-WLS'
        when _id = '5b73bbc2e1b2236ea64743dc' then 'USA-AL'
        when _id = '5b73bbdfe1b2236ea64743de' then 'USA-AK'
        when _id = '5b73bbe0e1b2236ea64743df' then 'USA-AZ'
        when _id = '5b73bbe0e1b2236ea64743e0' then 'USA-AR'
        when _id = '5b73bbe1e1b2236ea64743e1' then 'USA-CA'
        when _id = '5b73bbe1e1b2236ea64743e2' then 'USA-CO'
        when _id = '5b73bbe1e1b2236ea64743e3' then 'USA-CT'
        when _id = '5b73bbe1e1b2236ea64743e4' then 'USA-DE'
        when _id = '5b73bbe1e1b2236ea64743e5' then 'USA-DC'
        when _id = '5b73bbe6e1b2236ea64743e6' then 'USA-FL'
        when _id = '5b73bbe8e1b2236ea64743e7' then 'USA-GA'
        when _id = '5b73bbe8e1b2236ea64743e8' then 'USA-HI'
        when _id = '5b73bbe8e1b2236ea64743e9' then 'USA-ID'
        when _id = '5b73bbe8e1b2236ea64743ea' then 'USA-IL'
        when _id = '5b73bbe8e1b2236ea64743eb' then 'USA-IN'
        when _id = '5b73bbe8e1b2236ea64743ec' then 'USA-IA'
        when _id = '5b73bbe8e1b2236ea64743ed' then 'USA-KS'
        when _id = '5b73bbe8e1b2236ea64743ee' then 'USA-KY'
        when _id = '5b73bbeae1b2236ea64743ef' then 'USA-LA'
        when _id = '5b73bbece1b2236ea64743f0' then 'USA-ME'
        when _id = '5b73bbede1b2236ea64743f1' then 'LBR-MY'
        when _id = '5b73bbeee1b2236ea64743f2' then 'USA-MA'
        when _id = '5b73bbeee1b2236ea64743f3' then 'USA-MI'
        when _id = '5b73bbeee1b2236ea64743f4' then 'USA-MN'
        when _id = '5b73bbefe1b2236ea64743f5' then 'USA-MS'
        when _id = '5b73bbefe1b2236ea64743f6' then 'USA-MO'
        when _id = '5b73bbefe1b2236ea64743f7' then 'BGR-12'
        when _id = '5b73bbefe1b2236ea64743f8' then 'USA-NE'
        when _id = '5b73bbefe1b2236ea64743f9' then 'USA-NV'
        when _id = '5b73bbefe1b2236ea64743fa' then 'USA-NH'
        when _id = '5b73bbefe1b2236ea64743fb' then 'USA-NJ'
        when _id = '5b73bbefe1b2236ea64743fc' then 'USA-NM'
        when _id = '5b73bbf0e1b2236ea64743fd' then 'USA-NY'
        when _id = '5b73bbf1e1b2236ea64743fe' then 'USA-NC'
        when _id = '5b73bbf1e1b2236ea64743ff' then 'USA-ND'
        when _id = '5b73bbf1e1b2236ea6474400' then 'USA-OH'
        when _id = '5b73bbf1e1b2236ea6474401' then 'USA-OK'
        when _id = '5b73bbf1e1b2236ea6474402' then 'USA-OR'
        when _id = '5b73bbf2e1b2236ea6474403' then 'USA-PA'
        when _id = '5b73bbf2e1b2236ea6474404' then 'USA-RI'
        when _id = '5b73bbf2e1b2236ea6474405' then 'USA-SC'
        when _id = '5b73bbf2e1b2236ea6474406' then 'USA-SD'
        when _id = '5b73bbf2e1b2236ea6474407' then 'USA-TN'
        when _id = '5b73bbf3e1b2236ea6474408' then 'USA-TX'
        when _id = '5b73bbf4e1b2236ea6474409' then 'USA-UT'
        when _id = '5b73bbf4e1b2236ea647440a' then 'USA-VT'
        when _id = '5b73bbf4e1b2236ea647440b' then 'USA-VA'
        when _id = '5b73bbf5e1b2236ea647440c' then 'USA-WA'
        when _id = '5b73bbf6e1b2236ea647440d' then 'USA-WV'
        when _id = '5b73bbf6e1b2236ea647440e' then 'USA-WI'
        when _id = '5b73bbf6e1b2236ea647440f' then 'USA-WY'
        else string_agg(distinct coalesce(regionName_name, name_name))
      end as region_code
  from
    `datamart-finance.staging.v_hotel_core_region_flat` 
  where
    regionName_lang = 'en'
    and name_lang = 'en'
  group by
    _id
)
, hcct as (
  select
    distinct
    _id as country_id
    , string_agg(distinct replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(name_name), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(name_name, 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
                          r"[ùúûü]", 'u'),
                        r"[òóôöø]", 'o'),
                      r"[ìíîï]", 'i'),
                    r"[èéêë]", 'e'),
                  r"[àáâäå]", 'a'),
                r"[ÙÚÛÜ]", 'U'),
              r"[ÒÓÔÖØ]", 'O'),
            r"[ÌÍÎÏ]", 'I'),
          r"[ÈÉÊË]", 'E'),
        r"[ÀÁÂÄÅ]", 'A')
      ELSE
        (name_name)
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†','')) as country_name
    , case /*Mapping from country name into country code (ISO 3166-1 alpha-2)*/
        when _id = '5b73b922e1b2236ea6462402' then 'ID'
        when _id = '5b73ba73e1b2236ea6473e83' then 'VN'
        when _id = '5b73ba75e1b2236ea6473ec4' then 'JP'
        when _id = '5b73ba7ae1b2236ea6473f22' then 'ES'
        when _id = '5dea32bb676cc16b26312b6f' or _id = '5b73ba75e1b2236ea6473ebd' then 'GB'
        when _id = '5b73ba7be1b2236ea6473f3e' then 'GR'
        when _id = '5b73ba7ae1b2236ea6473f31' then 'NZ'
        when _id = '5b73b922e1b2236ea64623fe' then 'TH'
        when _id = '5b73ba76e1b2236ea6473edd' then 'TD'
        when _id = '5b73ba79e1b2236ea6473f02' then 'AU'
        when _id = '5b73ba74e1b2236ea6473ea4' then 'PH'
        when _id = '5b73ba79e1b2236ea6473f0f' then 'RU'
        when _id = '5b73b922e1b2236ea6462401' then 'MY'
        when _id = '5b73ba7ce1b2236ea6473f3f' then 'US'
        when _id = '5b73ba78e1b2236ea6473ef8' then 'OM'
        when _id = '5b73ba7ee1b2236ea6473f5d' then 'IN'
        when _id = '5b73ba78e1b2236ea6473f00' then 'TW'
        when _id = '5b73ba76e1b2236ea6473ed6' then 'MG'
        when _id = '5b73ba7ae1b2236ea6473f34' then 'MO'
        when _id = '5b73ba79e1b2236ea6473f0e' then 'SC'
        when _id = '5b73ba7ae1b2236ea6473f16' then 'WF'
        when _id = '5b73b922e1b2236ea64623ff' then 'SG'
        when _id = '5b73ba78e1b2236ea6473eea' then 'SA'
        when _id = '5b73ba7fe1b2236ea6473f6c' then 'BR'
        when _id = '5b73ba74e1b2236ea6473ea7' then 'BS'
        when _id = '5b73ba74e1b2236ea6473ea9' then 'IT'
        when _id = '5b73ba7ee1b2236ea6473f62' then 'LK'
        when _id = '5b73b922e1b2236ea6462404' then 'MV'
        when _id = '5b73ba76e1b2236ea6473ed4' then 'NP'
        when _id = '5b73ba7ce1b2236ea6473f44' then 'TL'
        when _id = '5b73ba7ce1b2236ea6473f45' then 'GE'
        when _id = '5b73ba7fe1b2236ea6473f6f' then 'PE'
        when _id = '5b73ba7fe1b2236ea6473f66' then 'TR'
        when _id = '5b73ba73e1b2236ea6473e90' then 'AE'
        when _id = '5b73ba74e1b2236ea6473e9d' then 'KH'
        when _id = '5b73ba79e1b2236ea6473f08' then 'LA'
        when _id = '5b73ba7fe1b2236ea6473f68' then 'CN'
        when _id = '5b73ba78e1b2236ea6473ee9' then 'CL'
        when _id = '5b73b922e1b2236ea6462400' then 'HK'
        when _id = '5b73ba7ce1b2236ea6473f53' then 'KP'
        when _id = '5b73ba76e1b2236ea6473ede' then 'KR'
        when _id = '5b73ba7ee1b2236ea6473f5a' then 'CA'
        when _id = '5b73ba75e1b2236ea6473ec5' then 'BN'
        when _id = '5b73ba74e1b2236ea6473eac' then 'MM'
        when _id = '5b73ba76e1b2236ea6473ed9' then 'BD'
        when _id = '5b73ba75e1b2236ea6473ebb' then 'BH'
        else ''
      end as country_code
  from
    `datamart-finance.staging.v_hotel_core_country_flat` 
  where
    name_lang = 'en'
    and countryName_lang = 'en'
  group by
    _id
)
, banks as (
  select
    id as bank_id
    , string_agg(distinct name) as bank_name
  from
    `datamart-finance.staging.v_banks`
  group by
    1
)
, hpi as (
  select
    distinct
    hotel_id as id
    , bank_name
    , bank_branch
    , account_number
    , account_holder_name
    , swift_code
  from
    `datamart-finance.staging.v_hotel_payment_informations` 
    left join banks using (bank_id)
)
, hpt as (
  select
    distinct
    business_id as hotel_id
    , type
  from
    `datamart-finance.staging.v_hotel__payment_type` 
  where
    status = 'active'
)
, htls as (
  select
    id as hotel_id_hb
    , string_agg(distinct coalesce(name,alias)) as hotel_name_hb
    , string_agg(distinct address) as hotel_address_hb
    , string_agg(distinct city_name) as hotel_city_hb
    , string_agg(distinct region_code) as hotel_region_hb
    , string_agg(distinct country_code) as hotel_country_hb
    , string_agg(distinct bank_name) as hotel_bank_name
    , string_agg(distinct bank_branch) as hotel_bank_branch
    , string_agg(distinct account_number) as hotel_account_number
    , string_agg(distinct account_holder_name) as hotel_account_holder_name
    , string_agg(distinct swift_code) as hotel_swift_code
    , string_agg(distinct postal_code) as hotel_postal_code
  from
    `datamart-finance.staging.v_hotels`
    left join hcc using (city_id)
    left join hcr using (region_id)
    left join hcct using (country_id)
    left join hpi using (id)
  where
    active_status >= 0
  group by
    1
)
, oth as (
  select
    order_id
    , string_agg(distinct safe_cast(hb.hotel_id_hb as string)) as supplier_id
    , string_agg(distinct htls.hotel_name_hb) as supplier_name
    , string_agg(distinct htls.hotel_address_hb) as address_name
    , string_agg(distinct htls.hotel_city_hb) as city_name
    , string_agg(distinct htls.hotel_region_hb) as region_code
    , string_agg(distinct htls.hotel_country_hb) as country_code
    , string_agg(distinct room_source) as room_source
    , string_agg(distinct hotel_bank_name) as Supplier_Bank_Name
    , string_agg(distinct hotel_bank_branch) as Supplier_Bank_Branch_Name
    , string_agg(distinct hotel_account_number) as Supplier_Bank_Account_Number
    , string_agg(distinct hotel_account_holder_name) as Supplier_Bank_Account_Name
    , string_agg(distinct hotel_swift_code) as Supplier_Bank_BIC_SWIFT_Code
    , string_agg(distinct hotel_postal_code) as Address_Postal_Code
    , 'Hotel' as product_category
  from
    `datamart-finance.staging.v_order__tixhotel` oth
    left join hb using (hotel_itinerarynumber)
    left join htls using (hotel_id_hb)
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp < (select filter3 from fd)
    and room_source = 'TIKET'
  group by
    1
)
, combine as (
  select
    distinct
    coalesce(decm.supplier_id, occar.supplier_id, oth.supplier_id) as Supplier_Reference_ID
    , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(trim(coalesce(oecm.supplier_name, occar.supplier_name, oth.supplier_name))), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(trim(coalesce(oecm.supplier_name, occar.supplier_name, oth.supplier_name)), 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
                          r"[ùúûü]", 'u'),
                        r"[òóôöø]", 'o'),
                      r"[ìíîï]", 'i'),
                    r"[èéêë]", 'e'),
                  r"[àáâäå]", 'a'),
                r"[ÙÚÛÜ]", 'U'),
              r"[ÒÓÔÖØ]", 'O'),
            r"[ÌÍÎÏ]", 'I'),
          r"[ÈÉÊË]", 'E'),
        r"[ÀÁÂÄÅ]", 'A')
      ELSE
        trim(coalesce(oecm.supplier_name, occar.supplier_name, oth.supplier_name))
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†','') AS Supplier_Name
    , coalesce(oecm.product_category, occar.product_category, oth.product_category) as Supplier_Category_ID
    , '' Supplier_Group_ID
    , '' Worktag_Product_Provider_Org_Ref_ID
    , coalesce(oecm.product_category, occar.product_category, oth.product_category) as Worktag_Product_Category_Ref_ID
    , '' Supplier_Default_Currency
    , 'Immediate' Payment_Terms
    , 'Deposit_Deduction' Accepted_Payment_Types_1
    , 'Credit_Card' Accepted_Payment_Types_2
    , 'PG_In_Transit' Accepted_Payment_Types_3
    , 'TT' Accepted_Payment_Types_4
    , '' Accepted_Payment_Types_5
    , case 
        when string_agg(distinct ocd.order_type) = 'car' then 
          case 
            when string_agg(distinct order_name_detail) like '%EXTRA%' or string_agg(distinct order_name_detail) like '%BEST PRICE%' then 'Deposit_Deduction'
            else 'TT'
          end
        when string_agg(distinct ocd.order_type) = 'event' then
          case
            when string_agg(distinct decm.ext_source_event) = 'BE_MY_GUEST' then 'Deposit_Deduction'
            else 'TT'
          end 
        when string_agg(distinct ocd.order_type) = 'tixhotel' then 
          case
            when string_agg(distinct hpt.type) = 'deposit' then 'Deposit_Deduction'
            when string_agg(distinct hpt.type) = 'creditcard' then 'Credit_Card'
            else 'TT'
          end
      end as Default_Payment_Type
    , '' Tax_Default_Tax_Code_ID
    , '' Tax_Default_Withholding_Tax_Code_ID
    , '' Tax_ID_NPWP
    , '' Tax_ID_Type
    , '' Transaction_Tax_YN
    , '' Primary_Tax_YN
    , case
        when length(trim(country_code)) > 1 then format_date("%Y-%m-%d", date(payment_datetime))
        else ''
      end as Address_Effective_Date
    , ifnull(country_code,'') as Address_Country_Code
    , coalesce(REPLACE(address_name,'\n','')) as Address_Line_1
    , '' Address_Line_2
    , '' Address_City_Subdivision_2
    , '' Address_City_Subdivision_1
    , ifnull(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(city_name), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(city_name, 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
                          r"[ùúûü]", 'u'),
                        r"[òóôöø]", 'o'),
                      r"[ìíîï]", 'i'),
                    r"[èéêë]", 'e'),
                  r"[àáâäå]", 'a'),
                r"[ÙÚÛÜ]", 'U'),
              r"[ÒÓÔÖØ]", 'O'),
            r"[ÌÍÎÏ]", 'I'),
          r"[ÈÉÊË]", 'E'),
        r"[ÀÁÂÄÅ]", 'A')
      ELSE
        (city_name)
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†',''), '') AS Address_City
    , '' Address_Region_Subdivision_2
    , '' Address_Region_Subdivision_1
    , ifnull(region_code,'') as Address_Region_Code
    , ifnull(Address_Postal_Code, '') Address_Postal_Code
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then (ifnull(country_code,''))
        else ''
      end as Supplier_Bank_Country
    , '' Supplier_Bank_Currency
    , '' Supplier_Bank_Account_Nickname
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then 'SA'
        else '' 
      end as Supplier_Bank_Account_Type
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then ifnull(Supplier_Bank_Name,'')
        else ''
      end as Supplier_Bank_Name
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then 'XXX'
        else ''
      end as Supplier_Bank_ID_Routing_Number
    , '' Supplier_Bank_Branch_ID
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then Supplier_Bank_Branch_Name
        else ''
      end as Supplier_Bank_Branch_Name
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then Supplier_Bank_Account_Number
        else ''
      end as Supplier_Bank_Account_Number
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 then Supplier_Bank_Account_Name
        else ''
      end as Supplier_Bank_Account_Name
    , case
        when length(trim(Supplier_Bank_Account_Number)) > 1 and REGEXP_CONTAINS(Supplier_Bank_BIC_SWIFT_Code, r'^[a-zA-Z]+$') then ifnull(upper(Supplier_Bank_BIC_SWIFT_Code), '')
        else ''
      end as Supplier_Bank_BIC_SWIFT_Code
    , max(payment_datetime) as max_payment_datetime
  from
    oc
    inner join ocd using (order_id)
    left join oth using (order_id)
    left join oecm using (order_detail_id)
    left join decm using (detail_event_id)
    left join occar using (order_detail_id)
    left join hpt on safe_cast(hpt.hotel_id as string) = oth.supplier_id
  where
    coalesce(oecm.product_category, occar.product_category, oth.product_category) is not null
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42
)
, add_row_number as (
  select
    *
    , row_number() over(partition by Supplier_Reference_ID order by max_payment_datetime desc) as rn
  from
    combine
)
, fact as (
  select
    * except (max_payment_datetime,rn)
  from
    add_row_number
  where
    rn = 1
    and (
      Supplier_Reference_ID is not null
      and length(Supplier_Reference_ID) > 0
      and Supplier_Reference_ID != '-'
      and Supplier_Reference_ID != '0'
    )
)
, ms as (
  select 
    * 
  from 
    `datamart-finance.datasource_workday.master_data_supplier`
)

select 
  fact.*
  , date(current_timestamp(),'Asia/Jakarta') as processed_date 
from 
  fact 
  left join ms on fact.Supplier_Reference_ID = ms.Supplier_Reference_ID 
where 
  ms.Supplier_Reference_ID is null