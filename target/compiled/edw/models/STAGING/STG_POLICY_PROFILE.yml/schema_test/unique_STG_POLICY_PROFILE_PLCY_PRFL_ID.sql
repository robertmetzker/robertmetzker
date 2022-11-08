
    
    



select count(*) as validation_errors
from (

    select
        PLCY_PRFL_ID

    from STAGING.STG_POLICY_PROFILE
    where PLCY_PRFL_ID is not null
    group by PLCY_PRFL_ID
    having count(*) > 1

) validation_errors


