
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_MEDICAL_INVOICE_PROCEDURES
where PRIMARY_SOURCE_SYSTEM is null


