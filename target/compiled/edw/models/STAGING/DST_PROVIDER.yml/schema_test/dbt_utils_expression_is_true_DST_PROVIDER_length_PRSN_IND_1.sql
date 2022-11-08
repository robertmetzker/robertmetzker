




with meet_condition as (

    select * from STAGING.DST_PROVIDER where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PRSN_IND) = 1)

)

select count(*)
from validation_errors

