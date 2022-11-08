




with meet_condition as (

    select * from STAGING.DST_CLAIM where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(CLM_NO) <= 15)

)

select count(*)
from validation_errors

