




with meet_condition as (

    select * from STAGING.DST_NDC_GPI_REFERENCE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(NDC_11_FORMATTED_CODE) = 13)

)

select count(*)
from validation_errors

