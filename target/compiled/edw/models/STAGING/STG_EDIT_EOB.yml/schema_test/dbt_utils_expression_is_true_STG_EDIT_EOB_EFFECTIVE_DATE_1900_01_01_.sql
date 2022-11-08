




with meet_condition as (

    select * from STAGING.STG_EDIT_EOB where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(EFFECTIVE_DATE >= '1900-01-01')

)

select count(*)
from validation_errors

