
    
    



select count(*) as validation_errors
from STAGING.STG_INVOICE_HEADER_EOB
where INVOICE_HEADER_ID is null


