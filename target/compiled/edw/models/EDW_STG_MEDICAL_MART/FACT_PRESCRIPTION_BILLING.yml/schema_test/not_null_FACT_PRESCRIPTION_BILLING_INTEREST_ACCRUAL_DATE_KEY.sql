
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_PRESCRIPTION_BILLING
where INTEREST_ACCRUAL_DATE_KEY is null


