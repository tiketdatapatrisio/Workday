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
, ms as (
  select 
    * 
  from 
    `datamart-finance.datasource_workday.master_data_customer`
)
/* b2b_corporate - start */
, ac as ( /*old b2b corp from gdocs*/
  select
    distinct(workday_business_id) as workday_business_id
    , REPLACE(REPLACE(company_name,'\r',''),'\n','') as Customer_Name
    , 'B2B_Corporate' as Customer_Category_ID
    , case
          when due_date = 7 then 'NET_7'
          when due_date = 14 then 'NET_14'
          when due_date = 30 then 'NET_30'
          when due_date = 45 then 'NET_45'
          when due_date = 0 then 'NET_14'
          when due_date is null then 'NET_14'
        end as Payment_Terms_ID
    , 'Manual' as Default_Payment_Type_ID
    , 'IDR' as Credit_Limit_Currency
    , credit_limit as Credit_Limit_Amount
    , case
          when npwp <> '' then 'TAX_CODE-6-1'
          when npwp = 'null' then ''
          else ''
          end as Tax_Default_Tax_Code
    , ifnull(replace(npwp,',','.'),'') as Tax_ID_NPWP
    , case 
          when npwp <> '' then 'IDN-NPWP'
          when npwp = 'null' then ''
          else ''
          end as Tax_ID_Type
    ,case 
          when npwp <> '' then 'Y'
          when npwp = 'null' then ''
          else ''
          end as Transaction_Tax_YN
    ,case 
          when npwp <> '' then 'Y'
          when npwp = 'null' then ''
          else ''
          end as Primary_Tax_YN
    ,case
          when address<>'' then '2020-01-01'
          when address='null' then ''
          else ''
          end as Address_Effective_Date
    , case
          when address<>'' then 'ID'
          when address='null' then ''
          else ''
          end as Address_Country_Code
    , ifnull(REPLACE(REPLACE(address,'\r',''),'\n',''), '') as Address_Line_1
    , '' as Address_Line_2
    , '' as Address_City_Subdivision_2
    , '' as Address_City_Subdivision_1
    , case 
        when address<>'' then 'ID'
        when address='null' then ''
        else ''
        end as Address_City
    , '' as Address_Region_Subdivision_2
    , '' as Address_Region_Subdivision_1
    , '' as Address_Region_Code
    , '' as Address_Postal_Code
  from
    `datamart-finance.staging.v_corporate_account` 
  where
    workday_business_id is not null
)
, vc as ( /*new b2b corp from dashboard*/
  select
  unique_id as workday_business_id
  , name as Customer_Name
  , 'B2B_Corporate' as Customer_Category_ID
  , case
      when due_date = 7 then 'NET_7'
      when due_date = 14 then 'NET_14'
      when due_date = 30 then 'NET_30'
      when due_date = 45 then 'NET_45'
      when due_date = 0 then 'NET_14'
      when due_date is null then 'NET_14'
    end as Payment_Terms_ID
  , 'Manual' as Default_Payment_Type_ID
  , 'IDR' as Credit_Limit_Currency
  , coalesce(credit_limit, 0) as Credit_Limit_Amount
  , '' as Tax_Default_Tax_Code
  , '' as Tax_ID_NPWP
  , '' as Tax_ID_Type
  , '' as Transaction_Tax_YN
  , '' as Primary_Tax_YN
  , case
      when billing_address <> '' then safe_cast(date(datetime(i.updated_at, 'Asia/Jakarta')) as string)
      when billing_address = 'null' then ''
      else ''
    end as Address_Effective_Date
  , case
      when billing_address <> '' then 'ID'
      when billing_address = 'null' then ''
      else ''
    end as Address_Country_Code
  , ifnull(billing_address,'') as Address_Line_1
  , '' as Address_Line_2
  , '' as Address_City_Subdivision_2
  , '' as Address_City_Subdivision_1
  , case 
      when billing_address<>'' then 'ID'
      when billing_address='null' then ''
      else ''
      end as Address_City
  , '' as Address_Region_Subdivision_2
  , '' as Address_Region_Subdivision_1
  , '' as Address_Region_Code
  , '' as Address_Postal_Code
from
  (
    select
      *
      , row_number() over(partition by id order by updated_at desc) as rn
    from 
      `datamart-finance.staging.v_tix_affiliate_platform__corporates`
  ) c
left join
  (
    select
      *
      , row_number() over(partition by corporate_id order by updated_at desc) as rn
    from
      `datamart-finance.staging.v_tix_affiliate_platform__corporate_infos`
  ) i
on c.id = i.corporate_id
left join
  `datamart-finance.staging.v_corporate_account`  ca
    on replace(lower(c.name),'pt ','')=ltrim(rtrim(replace(lower(ca.Company_Name),'pt ','')))
where
  c.rn = 1
  and i.rn = 1
  and unique_id is not null
  and is_active = 1
  and is_suspend = 0
)
, b2b_corp as (
select 
* 
except(rn)
  from (
  select 
  c.*
  , row_number() over(partition by workday_business_id order by workday_business_id) as rn
  , date(current_timestamp(), 'Asia/Jakarta') as processed_date
from
  (
    select * from ac
    union distinct
    select * from vc
    where workday_business_id not in 
    (
      select distinct workday_business_id from ac
    )
  ) as c 
  left join ms on c.workday_business_id = ms.Customer_Reference_ID 
  where ms.Customer_Reference_ID is null )
where rn = 1  
  /* b2b corporate - end */
)

/* b2b online and offline - start */
, oc as (
  select
    distinct
    case
      when reseller_type in ('tiket_agent','txtravel','agent','affiliate') then reseller_id
      when reseller_type in ('reseller','widget') then reseller_id
      else null
    end as Customer_Reference_ID
    , case
        when reseller_type in ('tiket_agent','txtravel','agent','affiliate') then 'B2B_Offline'
        when reseller_type in ('reseller','widget') then 'B2B_Online'
        else null
      end as Customer_Category_ID
  from
    `datamart-finance.staging.v_order__cart`
  where
    payment_status = 'paid'
    and payment_timestamp >= (select filter1 from fd)
    and payment_timestamp < (select filter3 from fd)
)
, bp as (
  select
    business_id as Customer_Reference_ID
    , business_name as Customer_Name
    , 'NET_14' as payment_terms_id
    , 'Manual' as default_payment_type_id
    , 'IDR' as credit_limit_currency
    , '' as credit_limit_amount
    , 'TAX_CODE-6-1' as tax_default_tax_code
    , '' as tax_id_npwp
    , '' as tax_id_type 
    , '' as transaction_tax_yn 
    , '' as primary_tax_yn 
    , case
        when business_address1<>'' then '2020-01-01'
        when business_address1='null' then ''
        else ''
        end  as Address_Effective_Date
    , case
        when business_address1<>'' then UPPER(business_country)
        when business_address1='null' then ''
        else ''
        end as Address_Country_Code
    , business_address1 as address_line_1
    , case
        when business_address1<>'' then business_address2
        when business_address1='null' then ''
        else ''
        end as address_line_2
    , '' as address_city_subdivision_2
    , '' as address_city_subdivision_1
    , case 
        when business_address1<>'' then UPPER(business_country)
        when business_address1='null' then ''
        else '' 
        end as address_city
    , '' as address_region_subdivision_2
    , '' as address_region_subdivision_1
    , '' as address_region_code
    , case
        when business_address1<>'' then business_zipcode
        when business_address1='null' then ''
        else ''
        end as address_postal_code
  from
    `datamart-finance.staging.v_business__profile` 
)
, fact as (
select
 *
from
  oc
  left join bp using (Customer_Reference_ID)
where
  Customer_Reference_ID is not null
)
, b2b_online as (
select 
  fact.*
  , date(current_timestamp(), 'Asia/Jakarta') as processed_date 
from 
  fact 
  left join ms on safe_cast(fact.Customer_Reference_ID as string) = ms.Customer_Reference_ID 
where 
  ms.Customer_Reference_ID is null
)
, fact_customer as(
select 
coalesce(safe_cast(workday_business_id as string),'') as Customer_Reference_ID
  , coalesce(safe_cast(Customer_Name as string), '') as Customer_Name
  , coalesce(safe_cast( Customer_Category_ID as string), '') as Customer_Category_ID
  , coalesce(safe_cast( Payment_Terms_ID as string), '') as Payment_Terms_ID
  , coalesce(safe_cast( Default_Payment_Type_ID as string), '') as Default_Payment_Type_ID
  , coalesce(safe_cast( Credit_Limit_Currency as string), '') as Credit_Limit_Currency
  , coalesce(safe_cast( Credit_Limit_Amount as string), '') as Credit_Limit_Amount
  , '' as Tax_Default_Tax_Code
  , '' as Tax_ID_NPWP
  , '' as Tax_ID_Type
  , '' as Transaction_Tax_YN
  , '' as Primary_Tax_YN
  , coalesce(safe_cast( Address_Effective_Date as string), '') as Address_Effective_Date
  , coalesce(safe_cast( Address_Country_Code as string), '') as Address_Country_Code
  , coalesce(safe_cast( Address_Line_1 as string), '') as Address_Line_1
  , coalesce(safe_cast( Address_Line_2 as string), '') as Address_Line_2
  , coalesce(safe_cast( Address_City_Subdivision_2 as string), '') as Address_City_Subdivision_2
  , coalesce(safe_cast( Address_City_Subdivision_1 as string), '') as Address_City_Subdivision_1
  , coalesce(safe_cast( Address_City as string), '') as Address_City
  , coalesce(safe_cast( Address_Region_Subdivision_2 as string), '') as Address_Region_Subdivision_2
  , coalesce(safe_cast( Address_Region_Subdivision_1 as string), '') as Address_Region_Subdivision_1
  , coalesce(safe_cast( Address_Region_Code as string), '') as Address_Region_Code
  , coalesce(safe_cast( Address_Postal_Code as string), '') as Address_Postal_Code
  , processed_date
from b2b_corp
UNION ALL 
select
  coalesce(safe_cast(Customer_Reference_ID as string),'') as Customer_Reference_ID
  , coalesce( safe_cast(Customer_Name as string), '') as Customer_Name
  , coalesce( safe_cast( Customer_Category_ID as string), '') as Customer_Category_ID
  , coalesce( safe_cast( Payment_Terms_ID as string), '') as Payment_Terms_ID
  , coalesce( safe_cast( Default_Payment_Type_ID as string), '') as Default_Payment_Type_ID
  , coalesce( safe_cast( Credit_Limit_Currency as string), '') as Credit_Limit_Currency
  , coalesce( safe_cast( Credit_Limit_Amount as string), '') as Credit_Limit_Amount
  , '' as Tax_Default_Tax_Code
  , '' as Tax_ID_NPWP
  , '' as Tax_ID_Type
  , '' as Transaction_Tax_YN
  , '' as Primary_Tax_YN
  , coalesce( safe_cast( Address_Effective_Date as string), '') as Address_Effective_Date
  , coalesce( safe_cast( Address_Country_Code as string), '') as Address_Country_Code
  , coalesce( safe_cast( Address_Line_1 as string), '') as Address_Line_1
  , coalesce( safe_cast( Address_Line_2 as string), '') as Address_Line_2
  , coalesce( safe_cast( Address_City_Subdivision_2 as string), '') as Address_City_Subdivision_2
  , coalesce( safe_cast( Address_City_Subdivision_1 as string), '') as Address_City_Subdivision_1
  , coalesce( safe_cast( Address_City as string), '') as Address_City
  , coalesce( safe_cast( Address_Region_Subdivision_2 as string), '') as Address_Region_Subdivision_2
  , coalesce( safe_cast( Address_Region_Subdivision_1 as string), '') as Address_Region_Subdivision_1
  , coalesce( safe_cast( Address_Region_Code as string), '') as Address_Region_Code
  , coalesce( safe_cast( Address_Postal_Code as string), '') as Address_Postal_Code
  , processed_date
from b2b_online 
)
select * from fact_customer