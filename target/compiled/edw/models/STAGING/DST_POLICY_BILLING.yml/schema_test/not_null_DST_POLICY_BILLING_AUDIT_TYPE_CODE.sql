
    
    



select count(*) as validation_errors
from STAGING.DST_POLICY_BILLING
where AUDIT_TYPE_CODE is null


