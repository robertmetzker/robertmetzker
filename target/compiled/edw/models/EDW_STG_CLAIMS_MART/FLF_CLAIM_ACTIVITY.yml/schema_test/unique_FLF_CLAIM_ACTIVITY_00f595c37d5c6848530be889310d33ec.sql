
    
    



select count(*) as validation_errors
from (

    select
        ACTIVITY_ID||'-'||ACTIVITY_DETAIL_ID||'-'||CLAIM_NUMBER

    from EDW_STG_CLAIMS_MART.FLF_CLAIM_ACTIVITY
    where ACTIVITY_ID||'-'||ACTIVITY_DETAIL_ID||'-'||CLAIM_NUMBER is not null
    group by ACTIVITY_ID||'-'||ACTIVITY_DETAIL_ID||'-'||CLAIM_NUMBER
    having count(*) > 1

) validation_errors


