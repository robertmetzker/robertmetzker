
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_WAGE_SOURCE
where AUDIT_USER_CREA_DTM is null

