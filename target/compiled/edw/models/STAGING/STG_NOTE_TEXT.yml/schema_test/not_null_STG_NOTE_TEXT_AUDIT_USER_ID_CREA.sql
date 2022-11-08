
    
    



select count(*) as validation_errors
from STAGING.STG_NOTE_TEXT
where AUDIT_USER_ID_CREA is null


