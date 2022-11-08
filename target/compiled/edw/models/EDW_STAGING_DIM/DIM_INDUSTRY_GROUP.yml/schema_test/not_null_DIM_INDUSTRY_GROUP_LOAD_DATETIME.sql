
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_INDUSTRY_GROUP
where LOAD_DATETIME is null


