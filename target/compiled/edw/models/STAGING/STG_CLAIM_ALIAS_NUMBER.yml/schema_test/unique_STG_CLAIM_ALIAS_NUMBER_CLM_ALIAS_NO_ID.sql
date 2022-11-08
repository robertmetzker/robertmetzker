
    
    



select count(*) as validation_errors
from (

    select
        CLM_ALIAS_NO_ID

    from STAGING.STG_CLAIM_ALIAS_NUMBER
    where CLM_ALIAS_NO_ID is not null
    group by CLM_ALIAS_NO_ID
    having count(*) > 1

) validation_errors


