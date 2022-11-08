
    
    



select count(*) as validation_errors
from STAGING.DST_NDC_GPI_REFERENCE
where NDC_4_PRODUCT_CODE is null


