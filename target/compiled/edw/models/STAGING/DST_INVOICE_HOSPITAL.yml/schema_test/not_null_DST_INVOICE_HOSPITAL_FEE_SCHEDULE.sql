
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_HOSPITAL
where FEE_SCHEDULE is null


