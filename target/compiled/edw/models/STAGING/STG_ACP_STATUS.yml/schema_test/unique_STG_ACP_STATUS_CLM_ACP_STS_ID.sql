
    
    



select count(*) as validation_errors
from (

    select
        CLM_ACP_STS_ID

    from STAGING.STG_ACP_STATUS
    where CLM_ACP_STS_ID is not null
    group by CLM_ACP_STS_ID
    having count(*) > 1

) validation_errors


