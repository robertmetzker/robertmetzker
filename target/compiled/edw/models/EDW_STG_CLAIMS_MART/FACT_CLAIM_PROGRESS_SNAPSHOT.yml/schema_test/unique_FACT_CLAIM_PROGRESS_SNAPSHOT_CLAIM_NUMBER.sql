
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_NUMBER

    from EDW_STG_CLAIMS_MART.FACT_CLAIM_PROGRESS_SNAPSHOT
    where CLAIM_NUMBER is not null
    group by CLAIM_NUMBER
    having count(*) > 1

) validation_errors


