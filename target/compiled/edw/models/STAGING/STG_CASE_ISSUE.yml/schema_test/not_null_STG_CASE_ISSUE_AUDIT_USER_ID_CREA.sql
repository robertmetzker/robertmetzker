
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_ISSUE
where AUDIT_USER_ID_CREA is null


