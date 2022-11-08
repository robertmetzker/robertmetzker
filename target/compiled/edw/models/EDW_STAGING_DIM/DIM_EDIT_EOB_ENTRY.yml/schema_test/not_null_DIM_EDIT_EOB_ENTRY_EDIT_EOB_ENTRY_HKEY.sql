
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_EDIT_EOB_ENTRY
where EDIT_EOB_ENTRY_HKEY is null


