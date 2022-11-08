




with meet_condition as (

    select * from EDW_STAGING_DIM.DIM_INVOICE_PROFILE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(LENGTH(IN_SUBROGATION_IND) = 1)

)

select count(*)
from validation_errors

