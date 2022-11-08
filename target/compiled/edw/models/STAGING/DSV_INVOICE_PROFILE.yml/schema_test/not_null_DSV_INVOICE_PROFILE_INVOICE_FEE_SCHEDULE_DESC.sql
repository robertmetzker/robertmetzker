
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_PROFILE
where INVOICE_FEE_SCHEDULE_DESC is null


