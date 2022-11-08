
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_PROFILE
where IN_SUBROGATION_IND is null


