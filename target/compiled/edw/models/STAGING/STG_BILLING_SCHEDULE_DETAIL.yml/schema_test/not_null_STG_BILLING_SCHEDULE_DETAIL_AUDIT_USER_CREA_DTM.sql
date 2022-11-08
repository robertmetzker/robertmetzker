
    
    



select count(*) as validation_errors
from STAGING.STG_BILLING_SCHEDULE_DETAIL
where AUDIT_USER_CREA_DTM is null


