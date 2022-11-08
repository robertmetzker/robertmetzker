
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_CONTROL_ELEMENT
where AUDIT_USER_CREA_DTM is null


