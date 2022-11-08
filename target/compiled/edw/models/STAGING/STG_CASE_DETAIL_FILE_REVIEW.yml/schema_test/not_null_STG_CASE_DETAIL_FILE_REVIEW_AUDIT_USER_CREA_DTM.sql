
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_DETAIL_FILE_REVIEW
where AUDIT_USER_CREA_DTM is null


