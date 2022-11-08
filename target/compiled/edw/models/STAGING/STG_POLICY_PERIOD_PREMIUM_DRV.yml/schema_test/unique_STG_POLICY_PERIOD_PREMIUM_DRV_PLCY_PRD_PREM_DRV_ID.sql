
    
    



select count(*) as validation_errors
from (

    select
        PLCY_PRD_PREM_DRV_ID

    from STAGING.STG_POLICY_PERIOD_PREMIUM_DRV
    where PLCY_PRD_PREM_DRV_ID is not null
    group by PLCY_PRD_PREM_DRV_ID
    having count(*) > 1

) validation_errors


