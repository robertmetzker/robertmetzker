
    
    



select count(*) as validation_errors
from (

    select
        CLM_AVG_WG_ID

    from STAGING.STG_CLAIM_AVERAGE_WAGE
    where CLM_AVG_WG_ID is not null
    group by CLM_AVG_WG_ID
    having count(*) > 1

) validation_errors


