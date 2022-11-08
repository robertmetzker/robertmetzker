




with meet_condition as (

    select * from STAGING.STG_CLAIM_FINANCIAL_TRANSACTION_APPLIED where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(FTAT_ID > 0)

)

select count(*)
from validation_errors

