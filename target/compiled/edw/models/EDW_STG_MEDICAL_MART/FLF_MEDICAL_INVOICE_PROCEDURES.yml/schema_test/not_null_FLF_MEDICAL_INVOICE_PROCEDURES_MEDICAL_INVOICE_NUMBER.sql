
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_MEDICAL_INVOICE_PROCEDURES
where MEDICAL_INVOICE_NUMBER is null


