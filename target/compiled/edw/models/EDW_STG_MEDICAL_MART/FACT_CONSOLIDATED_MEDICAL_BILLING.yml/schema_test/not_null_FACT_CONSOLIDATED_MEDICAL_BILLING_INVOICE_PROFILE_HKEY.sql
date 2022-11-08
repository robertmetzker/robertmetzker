
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_CONSOLIDATED_MEDICAL_BILLING
where INVOICE_PROFILE_HKEY is null


