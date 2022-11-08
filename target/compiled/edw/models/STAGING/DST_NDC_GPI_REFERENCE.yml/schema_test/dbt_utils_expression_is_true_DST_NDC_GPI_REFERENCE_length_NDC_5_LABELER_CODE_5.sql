




with meet_condition as (

    select * from STAGING.DST_NDC_GPI_REFERENCE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(NDC_5_LABELER_CODE) = 5)

)

select count(*)
from validation_errors

