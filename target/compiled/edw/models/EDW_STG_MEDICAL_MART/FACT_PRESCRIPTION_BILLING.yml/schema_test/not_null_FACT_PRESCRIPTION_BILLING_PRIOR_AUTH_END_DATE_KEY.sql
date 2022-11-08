
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_PRESCRIPTION_BILLING
where PRIOR_AUTH_END_DATE_KEY is null


