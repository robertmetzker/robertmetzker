




with meet_condition as (

    select * from STAGING.STG_NDC_VRSN_DURATION where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(FK_DCPR_CODE) = 4)

)

select count(*)
from validation_errors

