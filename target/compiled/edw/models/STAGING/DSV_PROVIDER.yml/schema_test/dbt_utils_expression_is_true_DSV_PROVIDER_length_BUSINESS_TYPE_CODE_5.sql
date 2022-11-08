




with meet_condition as (

    select * from STAGING.DSV_PROVIDER where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(BUSINESS_TYPE_CODE) = 5)

)

select count(*)
from validation_errors

