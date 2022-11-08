




with meet_condition as (

    select * from STAGING.STG_PROVIDER_TYPE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PRVDR_SFX_NMBR) = 4)

)

select count(*)
from validation_errors

