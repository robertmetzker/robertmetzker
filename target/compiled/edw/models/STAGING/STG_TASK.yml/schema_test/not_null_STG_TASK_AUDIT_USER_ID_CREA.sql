
    
    



select count(*) as validation_errors
from STAGING.STG_TASK
where AUDIT_USER_ID_CREA is null


