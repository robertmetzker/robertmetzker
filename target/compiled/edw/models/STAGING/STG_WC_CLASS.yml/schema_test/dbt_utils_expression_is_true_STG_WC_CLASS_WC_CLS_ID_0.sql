




with meet_condition as (

    select * from STAGING.STG_WC_CLASS where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(WC_CLS_ID > 0)

)

select count(*)
from validation_errors

