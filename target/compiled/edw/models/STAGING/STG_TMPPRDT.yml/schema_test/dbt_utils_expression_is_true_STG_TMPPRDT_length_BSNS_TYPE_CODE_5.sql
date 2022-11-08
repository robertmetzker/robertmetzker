




with meet_condition as (

    select * from STAGING.STG_TMPPRDT where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(BSNS_TYPE_CODE) <= 5)

)

select count(*)
from validation_errors

