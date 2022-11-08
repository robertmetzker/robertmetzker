
    
    



select count(*) as validation_errors
from STAGING.STG_BILLING_SCHEDULE_DETAIL_AMOUNT
where AUDIT_USER_ID_CREA is null


