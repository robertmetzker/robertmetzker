
    
    



select count(*) as validation_errors
from (

    select
        HISTORY_ID

    from EDW_STG_CLAIMS_MART.FLF_CLAIM_ICD_ALLOWANCE_DETAIL
    where HISTORY_ID is not null
    group by HISTORY_ID
    having count(*) > 1

) validation_errors


