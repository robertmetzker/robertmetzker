
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_HEALTHCARE_SERVICE_AUTHORIZATION
where LOAD_DATETIME is null


