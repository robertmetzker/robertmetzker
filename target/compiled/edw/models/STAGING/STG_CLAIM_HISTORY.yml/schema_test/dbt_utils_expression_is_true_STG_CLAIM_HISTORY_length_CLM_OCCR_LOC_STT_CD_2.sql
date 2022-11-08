




with meet_condition as (

    select * from STAGING.STG_CLAIM_HISTORY where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(CLM_OCCR_LOC_STT_CD) = 2)

)

select count(*)
from validation_errors

