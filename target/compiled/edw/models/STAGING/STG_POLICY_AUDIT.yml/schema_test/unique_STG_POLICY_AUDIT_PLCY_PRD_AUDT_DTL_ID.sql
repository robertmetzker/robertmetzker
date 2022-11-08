
    
    



select count(*) as validation_errors
from (

    select
        PLCY_PRD_AUDT_DTL_ID

    from STAGING.STG_POLICY_AUDIT
    where PLCY_PRD_AUDT_DTL_ID is not null
    group by PLCY_PRD_AUDT_DTL_ID
    having count(*) > 1

) validation_errors


