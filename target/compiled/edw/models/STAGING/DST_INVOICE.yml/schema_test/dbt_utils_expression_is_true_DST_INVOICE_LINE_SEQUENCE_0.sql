




with meet_condition as (

    select * from STAGING.DST_INVOICE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(LINE_SEQUENCE >= 0)

)

select count(*)
from validation_errors

