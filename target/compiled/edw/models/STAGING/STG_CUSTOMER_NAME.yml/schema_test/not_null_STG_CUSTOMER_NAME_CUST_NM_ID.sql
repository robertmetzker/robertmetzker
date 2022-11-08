
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_NAME
where CUST_NM_ID is null


