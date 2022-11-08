
    
    



select count(*) as validation_errors
from (

    select
        POLICY_STANDING_HKEY

    from EDW_STAGING_DIM.DIM_POLICY_STANDING
    where POLICY_STANDING_HKEY is not null
    group by POLICY_STANDING_HKEY
    having count(*) > 1

) validation_errors


