
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_HEALTHCARE_SERVICE
where PRIMARY_SOURCE_SYSTEM is null


