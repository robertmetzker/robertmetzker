
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_PROFESSIONAL_ASC_BILLING
where CPT_SERVICE_RENDERED_HKEY is null


