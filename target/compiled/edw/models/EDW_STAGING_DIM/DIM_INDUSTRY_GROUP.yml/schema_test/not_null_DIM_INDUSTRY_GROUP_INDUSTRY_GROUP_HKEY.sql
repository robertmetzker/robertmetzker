
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_INDUSTRY_GROUP
where INDUSTRY_GROUP_HKEY is null


