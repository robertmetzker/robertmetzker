
    
    



select count(*) as validation_errors
from (

    select
        RATING_ELEMENT_HKEY

    from EDW_STAGING_DIM.DIM_RATING_ELEMENT
    where RATING_ELEMENT_HKEY is not null
    group by RATING_ELEMENT_HKEY
    having count(*) > 1

) validation_errors


