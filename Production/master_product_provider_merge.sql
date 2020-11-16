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
, base as(
select *, row_number() over(partition by Organization_Reference_ID order by processed_date desc) row_num
from mpp_old
)

select * except(row_num)
from base
where row_num=1