




with meet_condition as (

    select * from STAGING.STG_EDIT_EOB where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(EDIT_RID >= 1)

)

select count(*)
from validation_errors

