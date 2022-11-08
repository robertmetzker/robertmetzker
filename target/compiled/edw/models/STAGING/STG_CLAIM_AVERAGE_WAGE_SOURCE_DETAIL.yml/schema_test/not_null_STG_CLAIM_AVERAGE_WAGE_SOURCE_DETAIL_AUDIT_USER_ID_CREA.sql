
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_AVERAGE_WAGE_SOURCE_DETAIL
where AUDIT_USER_ID_CREA is null


