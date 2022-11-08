




with meet_condition as (

    select * from STAGING.DST_PROVIDER_ENROLLMENT_STATUS where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(ENRL_STS_TYPE_CODE) <= 5)

)

select count(*)
from validation_errors

