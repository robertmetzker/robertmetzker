
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_EVENT
where AUDIT_USER_ID_CREA is null


