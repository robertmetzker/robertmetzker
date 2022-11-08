
    
    



select count(*) as validation_errors
from STAGING.STG_ASSIGNMENT
where AUDIT_USER_CREA_DTM is null


