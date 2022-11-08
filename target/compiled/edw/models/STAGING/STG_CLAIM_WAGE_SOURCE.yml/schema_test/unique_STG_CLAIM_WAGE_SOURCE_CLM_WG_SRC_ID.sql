
    
    



select count(*) as validation_errors
from (

    select
        CLM_WG_SRC_ID

    from STAGING.STG_CLAIM_WAGE_SOURCE
    where CLM_WG_SRC_ID is not null
    group by CLM_WG_SRC_ID
    having count(*) > 1

) validation_errors


