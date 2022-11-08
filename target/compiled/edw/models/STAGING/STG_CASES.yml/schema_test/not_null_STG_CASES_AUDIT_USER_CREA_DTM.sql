
    
    



select count(*) as validation_errors
from STAGING.STG_CASES
where AUDIT_USER_CREA_DTM is null


