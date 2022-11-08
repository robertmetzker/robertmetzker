
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_CONSOLIDATED_MEDICAL_BILLING
where HEALTHCARE_SERVICE_RENDERED_HKEY is null


