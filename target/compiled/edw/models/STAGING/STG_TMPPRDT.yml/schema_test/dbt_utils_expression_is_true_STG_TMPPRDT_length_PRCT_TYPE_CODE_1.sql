




with meet_condition as (

    select * from STAGING.STG_TMPPRDT where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PRCT_TYPE_CODE) = 1)

)

select count(*)
from validation_errors

