
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_HEALTHCARE_AUTHORIZATION_STATUS
where UNIQUE_ID_KEY is null


