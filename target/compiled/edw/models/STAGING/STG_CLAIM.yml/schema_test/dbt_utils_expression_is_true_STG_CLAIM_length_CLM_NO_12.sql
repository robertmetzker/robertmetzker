




with meet_condition as (

    select * from STAGING.STG_CLAIM where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(CLM_NO) < 12)

)

select count(*)
from validation_errors

