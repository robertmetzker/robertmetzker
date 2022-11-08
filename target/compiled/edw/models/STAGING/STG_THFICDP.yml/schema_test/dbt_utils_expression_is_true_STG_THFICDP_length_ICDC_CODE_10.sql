




with meet_condition as (

    select * from STAGING.STG_THFICDP where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(ICDC_CODE) <= 10)

)

select count(*)
from validation_errors

