
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_DETAIL_LEGAL
where AUDIT_USER_ID_CREA is null


