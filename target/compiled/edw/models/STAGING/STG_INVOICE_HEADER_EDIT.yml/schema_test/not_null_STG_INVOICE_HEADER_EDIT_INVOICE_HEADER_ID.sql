
    
    



select count(*) as validation_errors
from STAGING.STG_INVOICE_HEADER_EDIT
where INVOICE_HEADER_ID is null


