
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_MEDICAL_INVOICE_LINE_EDIT_EOB
where PRIMARY_SOURCE_SYSTEM is null


