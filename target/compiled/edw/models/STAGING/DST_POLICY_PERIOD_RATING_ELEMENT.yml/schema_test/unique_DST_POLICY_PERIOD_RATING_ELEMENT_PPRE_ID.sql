
    
    



select count(*) as validation_errors
from (

    select
        PPRE_ID

    from STAGING.DST_POLICY_PERIOD_RATING_ELEMENT
    where PPRE_ID is not null
    group by PPRE_ID
    having count(*) > 1

) validation_errors


