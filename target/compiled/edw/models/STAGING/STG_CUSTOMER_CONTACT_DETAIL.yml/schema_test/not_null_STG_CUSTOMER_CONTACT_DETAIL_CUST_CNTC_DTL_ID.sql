
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_CONTACT_DETAIL
where CUST_CNTC_DTL_ID is null


