
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PROFILE
where FEE_SCHEDULE is null


