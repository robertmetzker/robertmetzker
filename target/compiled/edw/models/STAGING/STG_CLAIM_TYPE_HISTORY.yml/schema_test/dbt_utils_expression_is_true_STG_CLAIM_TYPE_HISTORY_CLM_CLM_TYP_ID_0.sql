




with meet_condition as (

    select * from STAGING.STG_CLAIM_TYPE_HISTORY where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(CLM_CLM_TYP_ID > 0)

)

select count(*)
from validation_errors

