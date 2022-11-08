




with meet_condition as (

    select * from STAGING.STG_CASE_DETAIL_FILE_REVIEW where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(CDFR_FNL_IMPR_RT >= 0)

)

select count(*)
from validation_errors

