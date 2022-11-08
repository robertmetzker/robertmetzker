




with meet_condition as (

    select * from STAGING.DST_PROVIDER where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(GNDR_CODE) = 1)

)

select count(*)
from validation_errors

