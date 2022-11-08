
    
    



select count(*) as validation_errors
from (

    select
        CUST_SOC_SEC_RET_DTL_ID

    from STAGING.STG_CUSTOMER_SOC_SEC_RET_DTL
    where CUST_SOC_SEC_RET_DTL_ID is not null
    group by CUST_SOC_SEC_RET_DTL_ID
    having count(*) > 1

) validation_errors


