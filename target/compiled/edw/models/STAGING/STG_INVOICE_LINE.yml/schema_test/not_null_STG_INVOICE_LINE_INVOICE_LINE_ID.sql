
    
    



select count(*) as validation_errors
from STAGING.STG_INVOICE_LINE
where INVOICE_LINE_ID is null


