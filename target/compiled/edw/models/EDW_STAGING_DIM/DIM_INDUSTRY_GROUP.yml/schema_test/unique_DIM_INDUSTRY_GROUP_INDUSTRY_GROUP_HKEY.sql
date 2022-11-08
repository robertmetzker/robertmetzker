
    
    



select count(*) as validation_errors
from (

    select
        INDUSTRY_GROUP_HKEY

    from EDW_STAGING_DIM.DIM_INDUSTRY_GROUP
    where INDUSTRY_GROUP_HKEY is not null
    group by INDUSTRY_GROUP_HKEY
    having count(*) > 1

) validation_errors


