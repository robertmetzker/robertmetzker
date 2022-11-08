
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_WAGE_SOURCE_DETAIL
where AUDIT_USER_CREA_DTM is null


