
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_DETAIL_VOCATIONAL_REHAB
where AUDIT_USER_ID_CREA is null


