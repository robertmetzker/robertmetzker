
    
    



select count(*) as validation_errors
from STAGING.STG_NOTE
where AUDIT_USER_ID_CREA is null

