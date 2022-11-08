
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PROFILE
where SUBROGATION_FLAG is null


