
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_PRESCRIPTION_BILLING
where INVOICE_HEADER_CURRENT_STATUS_HKEY is null

