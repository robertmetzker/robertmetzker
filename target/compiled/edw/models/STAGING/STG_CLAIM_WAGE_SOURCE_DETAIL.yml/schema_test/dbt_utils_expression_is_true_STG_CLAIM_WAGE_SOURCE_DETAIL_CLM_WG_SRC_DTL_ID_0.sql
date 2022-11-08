




with meet_condition as (

    select * from STAGING.STG_CLAIM_WAGE_SOURCE_DETAIL where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(CLM_WG_SRC_DTL_ID > 0)

)

select count(*)
from validation_errors

