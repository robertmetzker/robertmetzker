




with meet_condition as (

    select * from STAGING.STG_BSDA_PLCY_FNCL_TRAN_XREF where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(BASAPFT_ID > 0)

)

select count(*)
from validation_errors

