
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_CHILD_SUPPORT
where CUST_ID_ISSR is null


