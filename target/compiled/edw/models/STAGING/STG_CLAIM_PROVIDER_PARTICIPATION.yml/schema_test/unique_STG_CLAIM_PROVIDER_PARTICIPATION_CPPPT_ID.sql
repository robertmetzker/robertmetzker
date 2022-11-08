
    
    



select count(*) as validation_errors
from (

    select
        CPPPT_ID

    from STAGING.STG_CLAIM_PROVIDER_PARTICIPATION
    where CPPPT_ID is not null
    group by CPPPT_ID
    having count(*) > 1

) validation_errors


