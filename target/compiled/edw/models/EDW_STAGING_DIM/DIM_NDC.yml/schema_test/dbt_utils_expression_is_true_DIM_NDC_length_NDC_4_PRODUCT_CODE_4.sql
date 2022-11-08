




with meet_condition as (

    select * from EDW_STAGING_DIM.DIM_NDC where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(NDC_4_PRODUCT_CODE) = 4)

)

select count(*)
from validation_errors

