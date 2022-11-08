




with meet_condition as (

    select * from STAGING.STG_PERSON_HISTORY where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(PRSN_TAX_EXMT_NO >= 0)

)

select count(*)
from validation_errors

