




with meet_condition as (

    select * from STAGING.DSV_NDC_GPI_REFERENCE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(NDC_11_CODE) = 11)

)

select count(*)
from validation_errors

