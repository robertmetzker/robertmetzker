
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PROVIDER
where PROVIDER_PEACH_NUMBER is null


