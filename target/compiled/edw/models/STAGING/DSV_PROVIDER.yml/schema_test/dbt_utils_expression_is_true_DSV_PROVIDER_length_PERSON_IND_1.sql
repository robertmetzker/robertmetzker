




with meet_condition as (

    select * from STAGING.DSV_PROVIDER where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PERSON_IND) = 1)

)

select count(*)
from validation_errors

