
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_WAGE_SOURCE
where AUDIT_USER_ID_CREA is null


