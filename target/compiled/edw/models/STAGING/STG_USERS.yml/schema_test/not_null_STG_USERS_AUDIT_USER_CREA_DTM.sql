
    
    



select count(*) as validation_errors
from STAGING.STG_USERS
where AUDIT_USER_CREA_DTM is null


