




with meet_condition as (

    select * from STAGING.STG_WC_CLASS where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(WC_CLS_CLS_CD) = 4)

)

select count(*)
from validation_errors

