
    
    



select count(*) as validation_errors
from (

    select
        ORGANIZATION_UNIT_HKEY

    from EDW_STAGING_DIM.DIM_ORGANIZATIONAL_UNIT
    where ORGANIZATION_UNIT_HKEY is not null
    group by ORGANIZATION_UNIT_HKEY
    having count(*) > 1

) validation_errors


