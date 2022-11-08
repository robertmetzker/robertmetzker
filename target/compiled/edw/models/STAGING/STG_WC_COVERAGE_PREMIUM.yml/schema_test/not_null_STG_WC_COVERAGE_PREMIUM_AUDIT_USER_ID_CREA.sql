
    
    



select count(*) as validation_errors
from STAGING.STG_WC_COVERAGE_PREMIUM
where AUDIT_USER_ID_CREA is null


