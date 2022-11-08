
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_PARTICIPATION
where AUDIT_USER_CREA_DTM is null


