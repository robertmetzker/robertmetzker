




with meet_condition as (

    select * from STAGING.STG_CASE_DETAIL_LEGAL where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(CASE_VENU_LOC_TYP_IC_CD) = 3)

)

select count(*)
from validation_errors

