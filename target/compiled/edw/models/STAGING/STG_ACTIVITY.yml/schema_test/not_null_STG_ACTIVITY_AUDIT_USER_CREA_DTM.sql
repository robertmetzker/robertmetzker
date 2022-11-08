
    
    



select count(*) as validation_errors
from STAGING.STG_ACTIVITY
where AUDIT_USER_CREA_DTM is null


