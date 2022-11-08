
    
    



select count(*) as validation_errors
from (

    select
        PARTICIPATION_ID

    from EDW_STG_CLAIMS_MART.FLF_CLAIM_PARTICIPATION
    where PARTICIPATION_ID is not null
    group by PARTICIPATION_ID
    having count(*) > 1

) validation_errors


