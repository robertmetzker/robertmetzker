
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_HEALTHCARE_AUTHORIZATION_STATUS
where LOAD_DATETIME is null


