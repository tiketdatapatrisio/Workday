with 
 ms_old as (
    select 
      supplier_id as Supplier_Reference_ID
      , supplier_name  as Supplier_Name
      , product_category  as Supplier_Category_ID
      , "" as Supplier_Group_ID
      , ""  as Worktag_Product_Provider_Org_Ref_ID
      , product_category  as Worktag_Product_Category_Ref_ID
      , ""  as Supplier_Default_Currency
      , "Immediate"  as Payment_Terms
      , "Deposit_Deduction"  as Accepted_Payment_Types_1
      , "Credit_Card"  as Accepted_Payment_Types_2
      , "PG_In_Transit"  as Accepted_Payment_Types_3
      , "TT"  as Accepted_Payment_Types_4
      , ""  as Accepted_Payment_Types_5
      , payment_type  as Default_Payment_Type
      , ""  as Tax_Default_Tax_Code_ID
      , ""  as Tax_Default_Withholding_Tax_Code_ID
      , ""  as Tax_ID_NPWP
      , ""  as Tax_ID_Type
      , ""  as Transaction_Tax_YN
      , ""  as Primary_Tax_YN
      , "2020-01-01"  as Address_Effective_Date
      , ""  as Address_Country_Code
      , address_name  as Address_Line_1
      , ""  as Address_Line_2
      , ""  as Address_City_Subdivision_2
      , ""  as Address_City_Subdivision_1
      , city_name  as Address_City
      , ""  as Address_Region_Subdivision_2
      , ""  as Address_Region_Subdivision_1
      , ""  as Address_Region_Code
      , ""  as Address_Postal_Code
      , ""  as Supplier_Bank_Country
      , ""  as Supplier_Bank_Currency
      , ""  as Supplier_Bank_Account_Nickname
      , ""  as Supplier_Bank_Account_Type
      , hotel_bank_name  as Supplier_Bank_Name
      , "XXX"  as Supplier_Bank_ID_Routing_Number
      , ""   as Supplier_Bank_Branch_ID
      , hotel_bank_branch  as Supplier_Bank_Branch_Name
      , hotel_account_number  as Supplier_Bank_Account_Number
      , hotel_account_holder_name  as Supplier_Bank_Account_Name
      , ""  as Supplier_Bank_BIC_SWIFT_Code
      , date(current_timestamp(),'Asia/Jakarta') as processed_date 
    from `datamart-finance.datasource_workday.master_supplier`
 )
, base as(
select *, row_number() over(partition by Supplier_Reference_ID order by processed_date desc) row_num
from ms_old
)

select * except(row_num)
from base
where row_num=1