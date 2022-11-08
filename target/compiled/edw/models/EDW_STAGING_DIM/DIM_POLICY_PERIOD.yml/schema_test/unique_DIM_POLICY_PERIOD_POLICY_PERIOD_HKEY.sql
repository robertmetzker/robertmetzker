
    
    



select count(*) as validation_errors
from (

    select
        POLICY_PERIOD_HKEY

    from EDW_STAGING_DIM.DIM_POLICY_PERIOD
    where POLICY_PERIOD_HKEY is not null
    group by POLICY_PERIOD_HKEY
    having count(*) > 1

) validation_errors


