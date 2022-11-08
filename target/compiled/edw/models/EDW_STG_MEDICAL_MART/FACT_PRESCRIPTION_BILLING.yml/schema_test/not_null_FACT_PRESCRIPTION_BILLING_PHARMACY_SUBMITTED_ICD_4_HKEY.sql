
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_PRESCRIPTION_BILLING
where PHARMACY_SUBMITTED_ICD_4_HKEY is null


