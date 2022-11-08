




with meet_condition as (

    select * from STAGING.STG_CLAIM_DISABILITY_MANAGEMENT where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(CLM_DISAB_MANG_ID >= 1)

)

select count(*)
from validation_errors

