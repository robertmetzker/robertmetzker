
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_PROFESSIONAL_ASC_BILLING
where CLAIM_TYPE_STATUS_HKEY is null


