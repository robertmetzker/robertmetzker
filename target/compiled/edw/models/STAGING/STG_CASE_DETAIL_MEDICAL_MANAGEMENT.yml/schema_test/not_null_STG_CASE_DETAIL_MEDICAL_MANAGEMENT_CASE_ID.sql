
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_DETAIL_MEDICAL_MANAGEMENT
where CASE_ID is null


