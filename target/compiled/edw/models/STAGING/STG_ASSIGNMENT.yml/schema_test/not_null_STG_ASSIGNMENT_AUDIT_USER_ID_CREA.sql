
    
    



select count(*) as validation_errors
from STAGING.STG_ASSIGNMENT
where AUDIT_USER_ID_CREA is null


