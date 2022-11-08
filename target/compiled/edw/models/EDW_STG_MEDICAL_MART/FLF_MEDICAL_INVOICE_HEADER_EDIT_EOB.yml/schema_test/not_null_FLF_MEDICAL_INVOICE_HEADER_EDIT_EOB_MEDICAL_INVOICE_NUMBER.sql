
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_MEDICAL_INVOICE_HEADER_EDIT_EOB
where MEDICAL_INVOICE_NUMBER is null


