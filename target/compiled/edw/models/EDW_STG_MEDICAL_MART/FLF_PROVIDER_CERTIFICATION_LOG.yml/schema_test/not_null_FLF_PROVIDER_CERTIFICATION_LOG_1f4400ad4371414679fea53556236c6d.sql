
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_PROVIDER_CERTIFICATION_LOG
where PROVIDER_HKEY || BWC_CERTIFICATION_STATUS_HKEY || DERIVED_EFFECTIVE_DATE_KEY || DERIVED_ENDING_DATE_KEY is null

