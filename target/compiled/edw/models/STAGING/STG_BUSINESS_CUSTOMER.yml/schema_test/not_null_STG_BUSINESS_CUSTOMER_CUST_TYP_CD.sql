
    
    



select count(*) as validation_errors
from STAGING.STG_BUSINESS_CUSTOMER
where CUST_TYP_CD is null


