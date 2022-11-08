
    
    



select count(*) as validation_errors
from STAGING.STG_WC_CLASS_RATE_TIER
where AUDIT_USER_ID_CREA is null


