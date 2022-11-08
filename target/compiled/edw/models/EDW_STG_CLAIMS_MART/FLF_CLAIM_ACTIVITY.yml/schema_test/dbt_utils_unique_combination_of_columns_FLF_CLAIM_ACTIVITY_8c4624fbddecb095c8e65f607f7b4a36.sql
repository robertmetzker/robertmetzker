





with validation_errors as (

    select
        ACTIVITY_ID, ACTIVITY_DETAIL_ID, CLAIM_NUMBER
    from EDW_STG_CLAIMS_MART.FLF_CLAIM_ACTIVITY

    group by ACTIVITY_ID, ACTIVITY_DETAIL_ID, CLAIM_NUMBER
    having count(*) > 1

)

select count(*)
from validation_errors


