
    
    



select count(*) as validation_errors
from (

    select
        PREMIUM_CALCULATION_TYPE_HKEY

    from EDW_STAGING_DIM.DIM_PREMIUM_CALCULATION_TYPE
    where PREMIUM_CALCULATION_TYPE_HKEY is not null
    group by PREMIUM_CALCULATION_TYPE_HKEY
    having count(*) > 1

) validation_errors


