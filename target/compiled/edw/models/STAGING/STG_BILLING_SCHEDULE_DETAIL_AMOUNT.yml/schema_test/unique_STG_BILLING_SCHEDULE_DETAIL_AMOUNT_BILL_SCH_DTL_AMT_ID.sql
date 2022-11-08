
    
    



select count(*) as validation_errors
from (

    select
        BILL_SCH_DTL_AMT_ID

    from STAGING.STG_BILLING_SCHEDULE_DETAIL_AMOUNT
    where BILL_SCH_DTL_AMT_ID is not null
    group by BILL_SCH_DTL_AMT_ID
    having count(*) > 1

) validation_errors


