
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_MEDICAL_INVOICE_PROCEDURES
where INVOICE_SERVICE_FROM_DATE_KEY is null


