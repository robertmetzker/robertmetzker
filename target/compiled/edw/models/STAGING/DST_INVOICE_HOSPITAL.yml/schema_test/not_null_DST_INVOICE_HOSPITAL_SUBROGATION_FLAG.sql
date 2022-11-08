
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_HOSPITAL
where SUBROGATION_FLAG is null


