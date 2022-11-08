
    
    



select count(*) as validation_errors
from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT
where AUDIT_USER_CREA_DTM is null


