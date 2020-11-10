with 
  mpp_old as (
    select 
      product_provider_id as Organization_Reference_ID
     , product_provider_name as Product_Provider_Name
     , product_category as Product_Provider_Hierarchy
     , product_category as Product_Category
     , processed_date 
    from `datamart-finance.datasource_workday.master_product_provider`
  )
select * from mpp_old