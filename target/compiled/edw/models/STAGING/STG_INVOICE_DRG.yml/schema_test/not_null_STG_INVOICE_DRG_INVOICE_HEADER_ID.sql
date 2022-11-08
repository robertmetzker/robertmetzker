
    
    



select count(*) as validation_errors
from STAGING.STG_INVOICE_DRG
where INVOICE_HEADER_ID is null


