
    
    



select count(*) as validation_errors
from STAGING.STG_NOTE_TEXT
where AUDIT_USER_CREA_DTM is null


