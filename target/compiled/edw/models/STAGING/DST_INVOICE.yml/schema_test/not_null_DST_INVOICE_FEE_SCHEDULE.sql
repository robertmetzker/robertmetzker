
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE
where FEE_SCHEDULE is null


