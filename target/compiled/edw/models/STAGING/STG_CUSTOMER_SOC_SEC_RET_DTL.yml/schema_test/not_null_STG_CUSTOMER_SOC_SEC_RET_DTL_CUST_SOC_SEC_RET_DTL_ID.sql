
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_SOC_SEC_RET_DTL
where CUST_SOC_SEC_RET_DTL_ID is null


