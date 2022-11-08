
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_EOB
where EOB_HKEY is null


