
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_DIAGNOSIS_RELATED_GROUP
where PRIMARY_SOURCE_SYSTEM is null


