




with meet_condition as (

    select * from STAGING.STG_CERTIFY_AG_FNCL_TRAN_DTL where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(PFT_ID > 0)

)

select count(*)
from validation_errors

