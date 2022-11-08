
    
    



select count(*) as validation_errors
from STAGING.STG_INVOICE_DIAGNOSIS
where INVOICE_HEADER_ID is null


