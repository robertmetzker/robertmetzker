




with meet_condition as (

    select * from STAGING.STG_ICD_PROCEDURE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(ICD_RID > 0)

)

select count(*)
from validation_errors

