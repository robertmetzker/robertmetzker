
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_DIAGNOSIS_RELATED_GROUP
where DRG_HKEY is null


