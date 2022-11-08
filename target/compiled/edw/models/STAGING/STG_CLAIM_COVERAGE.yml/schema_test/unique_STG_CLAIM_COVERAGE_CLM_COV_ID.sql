
    
    



select count(*) as validation_errors
from (

    select
        CLM_COV_ID

    from STAGING.STG_CLAIM_COVERAGE
    where CLM_COV_ID is not null
    group by CLM_COV_ID
    having count(*) > 1

) validation_errors


