
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_NDC
where NDC_GPI_HKEY is null


