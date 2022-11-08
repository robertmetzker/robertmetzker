
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PARTICIPATION_TYPE
where PARTICIPATION_TYPE_HKEY is null


