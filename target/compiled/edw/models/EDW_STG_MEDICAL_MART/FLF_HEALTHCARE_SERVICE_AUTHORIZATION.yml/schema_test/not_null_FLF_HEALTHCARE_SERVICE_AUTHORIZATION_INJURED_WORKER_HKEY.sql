
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_HEALTHCARE_SERVICE_AUTHORIZATION
where INJURED_WORKER_HKEY is null

