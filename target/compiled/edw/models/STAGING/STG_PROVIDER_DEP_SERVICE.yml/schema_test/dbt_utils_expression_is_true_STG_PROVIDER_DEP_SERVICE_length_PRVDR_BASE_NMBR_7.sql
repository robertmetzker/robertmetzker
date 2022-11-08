




with meet_condition as (

    select * from STAGING.STG_PROVIDER_DEP_SERVICE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PRVDR_BASE_NMBR) = 7)

)

select count(*)
from validation_errors

