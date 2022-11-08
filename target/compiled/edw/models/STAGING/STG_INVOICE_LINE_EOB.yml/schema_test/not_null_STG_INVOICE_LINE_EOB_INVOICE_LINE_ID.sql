
    
    



select count(*) as validation_errors
from STAGING.STG_INVOICE_LINE_EOB
where INVOICE_LINE_ID is null


