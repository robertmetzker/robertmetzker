
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_BENEFIT_TYPE
where BENEFIT_TYPE_HKEY is null


