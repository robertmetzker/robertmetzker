
    
    



select count(*) as validation_errors
from STAGING.STG_BILLING_SCHEDULE_DETAIL_AMOUNT
where BILL_SCH_DTL_AMT_ID is null


