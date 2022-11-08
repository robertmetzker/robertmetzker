
    
    



select count(*) as validation_errors
from (

    select
        CLM_RSRV_ID

    from STAGING.STG_CLAIM_RESERVE
    where CLM_RSRV_ID is not null
    group by CLM_RSRV_ID
    having count(*) > 1

) validation_errors


