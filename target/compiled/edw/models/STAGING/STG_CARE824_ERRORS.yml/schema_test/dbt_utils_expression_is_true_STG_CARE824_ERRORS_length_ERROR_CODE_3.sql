




with meet_condition as (

    select * from STAGING.STG_CARE824_ERRORS where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(ERROR_CODE) = 3)

)

select count(*)
from validation_errors

