
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PROVIDER_ENROLLMENT_STATUS
where UNIQUE_ID_KEY is null


