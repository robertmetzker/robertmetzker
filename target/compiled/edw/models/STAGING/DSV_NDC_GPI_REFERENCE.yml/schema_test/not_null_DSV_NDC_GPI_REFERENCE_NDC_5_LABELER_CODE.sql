
    
    



select count(*) as validation_errors
from STAGING.DSV_NDC_GPI_REFERENCE
where NDC_5_LABELER_CODE is null

