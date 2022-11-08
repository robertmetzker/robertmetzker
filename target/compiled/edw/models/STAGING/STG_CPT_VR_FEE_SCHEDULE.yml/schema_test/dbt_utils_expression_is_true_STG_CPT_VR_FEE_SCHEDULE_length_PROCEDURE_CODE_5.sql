




with meet_condition as (

    select * from STAGING.STG_CPT_VR_FEE_SCHEDULE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PROCEDURE_CODE) = 5)

)

select count(*)
from validation_errors

