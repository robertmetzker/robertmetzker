
    
    



select count(*) as validation_errors
from STAGING.STG_INVOICE_LINE_EDIT
where INVOICE_LINE_ID is null


