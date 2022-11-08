
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_PROFILE
where SUBROGATION_TYPE_DESC is null


