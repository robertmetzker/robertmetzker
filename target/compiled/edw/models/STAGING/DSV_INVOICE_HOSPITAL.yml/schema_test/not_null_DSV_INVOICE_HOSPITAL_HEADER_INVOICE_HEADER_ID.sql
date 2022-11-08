
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_HOSPITAL
where HEADER_INVOICE_HEADER_ID is null


