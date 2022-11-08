
    
    



select count(*) as validation_errors
from (

    select
        YEAR_CONTROL_ELEMENT_HKEY

    from EDW_STAGING_DIM.DIM_YEAR_CONTROL_ELEMENT
    where YEAR_CONTROL_ELEMENT_HKEY is not null
    group by YEAR_CONTROL_ELEMENT_HKEY
    having count(*) > 1

) validation_errors


