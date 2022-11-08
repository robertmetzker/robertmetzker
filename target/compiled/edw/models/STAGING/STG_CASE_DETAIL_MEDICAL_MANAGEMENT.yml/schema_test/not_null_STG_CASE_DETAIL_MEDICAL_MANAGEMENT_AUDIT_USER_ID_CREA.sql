
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_DETAIL_MEDICAL_MANAGEMENT
where AUDIT_USER_ID_CREA is null


