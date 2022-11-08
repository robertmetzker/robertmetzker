
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_HEALTHCARE_SERVICE
where UNIQUE_ID_KEY is null


