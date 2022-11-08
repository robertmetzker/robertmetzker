




with meet_condition as (

    select * from STAGING.DST_PLACE_OF_SERVICE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(LENGTH(PLACE_OF_SERVICE_CODE)=2)

)

select count(*)
from validation_errors

