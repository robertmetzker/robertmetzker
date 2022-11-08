
    
    



select count(*) as validation_errors
from (

    select
        DISABILITY_TYPE_HKEY

    from EDW_STAGING_DIM.DIM_DISABILITY_TYPE
    where DISABILITY_TYPE_HKEY is not null
    group by DISABILITY_TYPE_HKEY
    having count(*) > 1

) validation_errors


