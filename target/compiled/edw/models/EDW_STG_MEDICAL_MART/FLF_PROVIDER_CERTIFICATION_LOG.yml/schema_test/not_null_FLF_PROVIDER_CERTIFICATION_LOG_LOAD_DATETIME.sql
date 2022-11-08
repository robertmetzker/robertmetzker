
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_PROVIDER_CERTIFICATION_LOG
where LOAD_DATETIME is null


