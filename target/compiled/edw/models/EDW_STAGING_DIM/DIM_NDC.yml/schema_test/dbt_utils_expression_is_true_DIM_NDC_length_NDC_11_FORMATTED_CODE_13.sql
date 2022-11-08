




with meet_condition as (

    select * from EDW_STAGING_DIM.DIM_NDC where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(NDC_11_FORMATTED_CODE) = 13)

)

select count(*)
from validation_errors

