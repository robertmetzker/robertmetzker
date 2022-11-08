
    
    



select count(*) as validation_errors
from (

    select
        BENEFIT_TYPE_HKEY

    from EDW_STAGING_DIM.DIM_BENEFIT_TYPE
    where BENEFIT_TYPE_HKEY is not null
    group by BENEFIT_TYPE_HKEY
    having count(*) > 1

) validation_errors


