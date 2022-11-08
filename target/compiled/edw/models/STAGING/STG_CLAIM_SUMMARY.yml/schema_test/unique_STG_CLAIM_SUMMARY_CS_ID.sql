
    
    



select count(*) as validation_errors
from (

    select
        CS_ID

    from STAGING.STG_CLAIM_SUMMARY
    where CS_ID is not null
    group by CS_ID
    having count(*) > 1

) validation_errors


