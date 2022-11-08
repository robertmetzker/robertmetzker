
    
    



select count(*) as validation_errors
from STAGING.STG_HOSPITAL_PROCEDURE
where INVOICE_HEADER_ID is null


