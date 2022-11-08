
    
    



select count(*) as validation_errors
from STAGING.DSV_NDC_GPI_REFERENCE
where NDC_11_CODE is null


