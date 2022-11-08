
    
    



select count(*) as validation_errors
from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL
where AUDIT_USER_ID_CREA is null


