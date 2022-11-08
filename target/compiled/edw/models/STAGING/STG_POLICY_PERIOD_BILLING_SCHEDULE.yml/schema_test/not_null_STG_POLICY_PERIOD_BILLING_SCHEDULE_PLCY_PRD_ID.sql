
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_PERIOD_BILLING_SCHEDULE
where PLCY_PRD_ID is null


