
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_PRESCRIPTION_BILLING
where PRESCRIPTION_WRITTEN_DATE_KEY is null


