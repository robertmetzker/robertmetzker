




with meet_condition as (

    select * from STAGING.DST_CPT_CODE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(SERVICE_CODE) = 5)

)

select count(*)
from validation_errors

