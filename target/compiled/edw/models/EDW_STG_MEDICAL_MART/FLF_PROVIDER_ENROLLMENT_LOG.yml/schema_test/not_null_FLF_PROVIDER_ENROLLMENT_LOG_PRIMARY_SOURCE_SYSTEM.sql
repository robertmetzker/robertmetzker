
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_PROVIDER_ENROLLMENT_LOG
where PRIMARY_SOURCE_SYSTEM is null


