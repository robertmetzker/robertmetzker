
    
    



select count(*) as validation_errors
from STAGING.STG_BILLING_SCHEDULE_DETAIL
where PLCY_PRD_BILL_SCH_ID is null


