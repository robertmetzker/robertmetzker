
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_HEALTHCARE_SERVICE_AUTHORIZATION
where CLAIM_TYPE_STATUS_HKEY is null


