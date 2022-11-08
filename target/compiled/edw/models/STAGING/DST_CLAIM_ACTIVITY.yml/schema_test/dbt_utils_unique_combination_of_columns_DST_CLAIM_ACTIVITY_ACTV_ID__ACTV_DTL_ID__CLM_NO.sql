





with validation_errors as (

    select
        ACTV_ID, ACTV_DTL_ID, CLM_NO
    from STAGING.DST_CLAIM_ACTIVITY

    group by ACTV_ID, ACTV_DTL_ID, CLM_NO
    having count(*) > 1

)

select count(*)
from validation_errors


