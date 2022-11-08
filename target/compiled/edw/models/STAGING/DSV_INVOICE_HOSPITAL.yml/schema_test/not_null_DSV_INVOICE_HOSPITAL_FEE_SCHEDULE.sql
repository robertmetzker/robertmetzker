
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_HOSPITAL
where FEE_SCHEDULE is null


