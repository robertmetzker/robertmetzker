
    
    



select count(*) as validation_errors
from STAGING.STG_QUOTE
where AUDIT_USER_ID_CREA is null


