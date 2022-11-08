
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_PERIOD
where AUDIT_USER_CREA_DTM is null


