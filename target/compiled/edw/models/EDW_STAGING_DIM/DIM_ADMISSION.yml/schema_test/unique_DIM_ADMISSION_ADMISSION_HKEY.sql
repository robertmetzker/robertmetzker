
    
    



select count(*) as validation_errors
from (

    select
        ADMISSION_HKEY

    from EDW_STAGING_DIM.DIM_ADMISSION
    where ADMISSION_HKEY is not null
    group by ADMISSION_HKEY
    having count(*) > 1

) validation_errors


