
    
    



select count(*) as validation_errors
from (

    select
        AGRE_ID

    from STAGING.STG_CLAIM
    where AGRE_ID is not null
    group by AGRE_ID
    having count(*) > 1

) validation_errors


