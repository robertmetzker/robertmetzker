
    
    



select count(*) as validation_errors
from (

    select
        MANUAL_CLASS_HKEY

    from EDW_STAGING_DIM.DIM_MANUAL_CLASSIFICATION
    where MANUAL_CLASS_HKEY is not null
    group by MANUAL_CLASS_HKEY
    having count(*) > 1

) validation_errors


