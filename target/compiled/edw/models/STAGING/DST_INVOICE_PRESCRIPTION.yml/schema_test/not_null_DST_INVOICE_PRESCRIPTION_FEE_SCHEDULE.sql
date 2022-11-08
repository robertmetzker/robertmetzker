
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PRESCRIPTION
where FEE_SCHEDULE is null


