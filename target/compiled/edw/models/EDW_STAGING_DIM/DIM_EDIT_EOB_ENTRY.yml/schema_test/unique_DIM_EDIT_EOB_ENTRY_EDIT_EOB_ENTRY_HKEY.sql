
    
    



select count(*) as validation_errors
from (

    select
        EDIT_EOB_ENTRY_HKEY

    from EDW_STAGING_DIM.DIM_EDIT_EOB_ENTRY
    where EDIT_EOB_ENTRY_HKEY is not null
    group by EDIT_EOB_ENTRY_HKEY
    having count(*) > 1

) validation_errors


