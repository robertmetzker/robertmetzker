




with meet_condition as (

    select * from STAGING.STG_CASE_DETAIL_MEDICAL_MANAGEMENT where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(CDMM_ID > 0)

)

select count(*)
from validation_errors

