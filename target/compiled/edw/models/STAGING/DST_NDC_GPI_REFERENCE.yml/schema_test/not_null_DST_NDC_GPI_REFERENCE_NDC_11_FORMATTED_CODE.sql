
    
    



select count(*) as validation_errors
from STAGING.DST_NDC_GPI_REFERENCE
where NDC_11_FORMATTED_CODE is null


