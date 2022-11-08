
    
    



select count(*) as validation_errors
from (

    select
        PLCY_PRD_BILL_SCH_ID

    from STAGING.STG_POLICY_PERIOD_BILLING_SCHEDULE
    where PLCY_PRD_BILL_SCH_ID is not null
    group by PLCY_PRD_BILL_SCH_ID
    having count(*) > 1

) validation_errors


