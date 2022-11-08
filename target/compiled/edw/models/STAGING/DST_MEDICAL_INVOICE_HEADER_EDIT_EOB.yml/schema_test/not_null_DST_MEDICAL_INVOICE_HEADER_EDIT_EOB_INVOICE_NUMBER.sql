
    
    



select count(*) as validation_errors
from STAGING.DST_MEDICAL_INVOICE_HEADER_EDIT_EOB
where INVOICE_NUMBER is null


