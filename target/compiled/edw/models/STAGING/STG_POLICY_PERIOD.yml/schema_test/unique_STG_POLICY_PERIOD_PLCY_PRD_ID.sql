
    
    



select count(*) as validation_errors
from (

    select
        PLCY_PRD_ID

    from STAGING.STG_POLICY_PERIOD
    where PLCY_PRD_ID is not null
    group by PLCY_PRD_ID
    having count(*) > 1

) validation_errors


