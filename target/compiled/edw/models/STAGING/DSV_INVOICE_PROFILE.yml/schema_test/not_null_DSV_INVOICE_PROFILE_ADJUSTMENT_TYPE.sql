
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_PROFILE
where ADJUSTMENT_TYPE is null


