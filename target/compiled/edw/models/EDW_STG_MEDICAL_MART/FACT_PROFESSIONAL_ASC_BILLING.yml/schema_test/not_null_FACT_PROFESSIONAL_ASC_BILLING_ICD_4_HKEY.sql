
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_PROFESSIONAL_ASC_BILLING
where ICD_4_HKEY is null


