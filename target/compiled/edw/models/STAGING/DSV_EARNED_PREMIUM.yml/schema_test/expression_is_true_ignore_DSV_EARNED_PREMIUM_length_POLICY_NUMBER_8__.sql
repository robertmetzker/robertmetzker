





with meet_condition as (

    select * from STAGING.DSV_EARNED_PREMIUM  
    
),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(POLICY_NUMBER) = 8)

)

select count(*)
from validation_errors

