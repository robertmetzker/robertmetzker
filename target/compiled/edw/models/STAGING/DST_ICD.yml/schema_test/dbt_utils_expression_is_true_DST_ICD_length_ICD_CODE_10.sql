




with meet_condition as (

    select * from STAGING.DST_ICD where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(ICD_CODE) <= 10)

)

select count(*)
from validation_errors

