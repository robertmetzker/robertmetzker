
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PRESCRIPTION
where SUBROGATION_FLAG is null


