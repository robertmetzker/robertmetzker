
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PROVIDER
where UNIQUE_ID_KEY is null


