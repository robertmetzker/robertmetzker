
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_PROFILE
where AUDIT_USER_ID_CREA is null


