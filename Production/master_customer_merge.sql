with 
  b2b_corporate as (
    select 
      cast(business_id as string) as Customer_Reference_ID	
      , REPLACE(REPLACE(company_name,'\r',''),'\n','') as Customer_Name
      , "B2B_Corporate" as Customer_Category_ID
      , "" as Payment_Terms_ID
      , "" as Default_Payment_Type_ID
      , "" as Credit_Limit_Currency
      , "" as Credit_Limit_Amount
      , "" as Tax_Default_Tax_Code
      , "" as Tax_ID_NPWP
      , "" as Tax_ID_Type
      , "" as Transaction_Tax_YN
      , "" as Primary_Tax_YN
      , "" as Address_Effective_Date
      , "" as Address_Country_Code
      , "" as Address_Line_1
      , "" as Address_Line_2
      , "" as Address_City_Subdivision_2
      , "" as Address_City_Subdivision_1
      , "" as Address_City
      , "" as Address_Region_Subdivision_2
      , "" as Address_Region_Subdivision_1
      , "" as Address_Region_Code
      , "" as Address_Postal_Code
      , processed_date
    from `datamart-finance.datasource_workday.master_b2b_corporate`  
  )
  , b2b_online_offline as (
    select 
      cast(business_id as string) as Customer_Reference_ID	
      , REPLACE(REPLACE(business_name,'\r',''),'\n','') as Customer_Name
      , customer_type as Customer_Category_ID
      , "" as Payment_Terms_ID
      , "" as Default_Payment_Type_ID
      , "" as Credit_Limit_Currency
      , "" as Credit_Limit_Amount
      , "" as Tax_Default_Tax_Code
      , "" as Tax_ID_NPWP
      , "" as Tax_ID_Type
      , "" as Transaction_Tax_YN
      , "" as Primary_Tax_YN
      , "" as Address_Effective_Date
      , "" as Address_Country_Code
      , "" as Address_Line_1
      , "" as Address_Line_2
      , "" as Address_City_Subdivision_2
      , "" as Address_City_Subdivision_1
      , "" as Address_City
      , "" as Address_Region_Subdivision_2
      , "" as Address_Region_Subdivision_1
      , "" as Address_Region_Code
      , "" as Address_Postal_Code
      , processed_date	
    from `datamart-finance.datasource_workday.master_b2b_online_and_offline` 
  )
  , group_customer as (
    select * from b2b_corporate
    union all
    select * from b2b_online_offline
  )
select * from group_customer