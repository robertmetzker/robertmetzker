




with meet_condition as (

    select * from STAGING.STG_PROVIDER_DEP_SERVICE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(DEP_SRVC_CODE) = 1)

)

select count(*)
from validation_errors

