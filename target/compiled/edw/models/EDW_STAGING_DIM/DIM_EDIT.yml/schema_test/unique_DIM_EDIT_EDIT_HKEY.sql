
    
    



select count(*) as validation_errors
from (

    select
        EDIT_HKEY

    from EDW_STAGING_DIM.DIM_EDIT
    where EDIT_HKEY is not null
    group by EDIT_HKEY
    having count(*) > 1

) validation_errors


