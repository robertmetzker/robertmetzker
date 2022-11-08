




with meet_condition as (

    select * from EDW_STG_POLICY_MART.FACT_EARNED_PREMIUM where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(POLICY_NUMBER) = 8)

)

select count(*)
from validation_errors

