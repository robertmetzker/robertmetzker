
    
    



select count(*) as validation_errors
from (

    select
        WRITTEN_PREMIUM_ELEMENT_HKEY

    from EDW_STAGING_DIM.DIM_WRITTEN_PREMIUM_ELEMENT
    where WRITTEN_PREMIUM_ELEMENT_HKEY is not null
    group by WRITTEN_PREMIUM_ELEMENT_HKEY
    having count(*) > 1

) validation_errors


