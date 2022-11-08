
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_MEDICAL_INVOICE_MONTHLY_SUMMARY
where INVOICE_PROFILE_HKEY is null


