




with meet_condition as (

    select * from STAGING.STG_FINANCIAL_TRANSACTION_TYPE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(FNCL_TRAN_TYP_ID > 0)

)

select count(*)
from validation_errors

