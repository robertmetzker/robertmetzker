
    
    



select count(*) as validation_errors
from STAGING.DST_MEDICAL_INVOICE_LINE_EDIT_EOB
where INV_INVOICE_LINE_ID is null


