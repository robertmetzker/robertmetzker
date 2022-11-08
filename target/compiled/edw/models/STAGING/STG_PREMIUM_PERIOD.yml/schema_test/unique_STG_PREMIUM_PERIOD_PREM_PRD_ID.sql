
    
    



select count(*) as validation_errors
from (

    select
        PREM_PRD_ID

    from STAGING.STG_PREMIUM_PERIOD
    where PREM_PRD_ID is not null
    group by PREM_PRD_ID
    having count(*) > 1

) validation_errors


