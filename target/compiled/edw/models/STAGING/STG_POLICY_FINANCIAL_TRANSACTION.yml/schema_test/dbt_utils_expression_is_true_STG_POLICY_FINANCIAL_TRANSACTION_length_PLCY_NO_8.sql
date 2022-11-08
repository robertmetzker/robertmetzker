




with meet_condition as (

    select * from STAGING.STG_POLICY_FINANCIAL_TRANSACTION where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PLCY_NO) = 8)

)

select count(*)
from validation_errors

