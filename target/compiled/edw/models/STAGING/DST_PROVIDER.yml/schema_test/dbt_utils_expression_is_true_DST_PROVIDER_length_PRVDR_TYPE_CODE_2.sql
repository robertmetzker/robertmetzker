




with meet_condition as (

    select * from STAGING.DST_PROVIDER where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PRVDR_TYPE_CODE) = 2)

)

select count(*)
from validation_errors

