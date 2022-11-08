
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_MEDICAL_INVOICE_LINE_EDIT_EOB
where MEDICAL_INVOICE_NUMBER is null


