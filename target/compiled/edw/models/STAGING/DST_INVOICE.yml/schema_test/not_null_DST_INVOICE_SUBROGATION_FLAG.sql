
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE
where SUBROGATION_FLAG is null


