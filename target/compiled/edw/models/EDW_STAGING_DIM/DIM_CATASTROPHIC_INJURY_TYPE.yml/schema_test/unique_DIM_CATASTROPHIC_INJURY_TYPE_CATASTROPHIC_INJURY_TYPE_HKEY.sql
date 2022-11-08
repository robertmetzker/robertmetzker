
    
    



select count(*) as validation_errors
from (

    select
        CATASTROPHIC_INJURY_TYPE_HKEY

    from EDW_STAGING_DIM.DIM_CATASTROPHIC_INJURY_TYPE
    where CATASTROPHIC_INJURY_TYPE_HKEY is not null
    group by CATASTROPHIC_INJURY_TYPE_HKEY
    having count(*) > 1

) validation_errors


