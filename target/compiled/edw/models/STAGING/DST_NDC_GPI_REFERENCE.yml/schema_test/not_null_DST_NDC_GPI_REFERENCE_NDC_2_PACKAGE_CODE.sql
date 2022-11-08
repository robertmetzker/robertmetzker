
    
    



select count(*) as validation_errors
from STAGING.DST_NDC_GPI_REFERENCE
where NDC_2_PACKAGE_CODE is null


