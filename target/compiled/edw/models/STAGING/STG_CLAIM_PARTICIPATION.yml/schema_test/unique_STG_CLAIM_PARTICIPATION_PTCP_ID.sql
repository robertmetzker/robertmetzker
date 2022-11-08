
    
    



select count(*) as validation_errors
from (

    select
        PTCP_ID

    from STAGING.STG_CLAIM_PARTICIPATION
    where PTCP_ID is not null
    group by PTCP_ID
    having count(*) > 1

) validation_errors


