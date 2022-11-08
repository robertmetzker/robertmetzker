




with meet_condition as (

    select * from STAGING.STG_WC_COVERAGE_PREMIUM where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(WC_COV_PREM_NO > 0)

)

select count(*)
from validation_errors

