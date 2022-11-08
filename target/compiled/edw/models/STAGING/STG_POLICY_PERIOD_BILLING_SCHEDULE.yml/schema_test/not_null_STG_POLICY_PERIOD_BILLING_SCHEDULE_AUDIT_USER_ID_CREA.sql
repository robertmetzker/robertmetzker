
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_PERIOD_BILLING_SCHEDULE
where AUDIT_USER_ID_CREA is null


