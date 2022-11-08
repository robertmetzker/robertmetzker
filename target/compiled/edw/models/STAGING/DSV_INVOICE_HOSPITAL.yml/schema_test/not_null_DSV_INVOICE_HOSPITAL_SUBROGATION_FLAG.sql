
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_HOSPITAL
where SUBROGATION_FLAG is null


