
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_DETAIL_LEGAL
where AUDIT_USER_CREA_DTM is null


