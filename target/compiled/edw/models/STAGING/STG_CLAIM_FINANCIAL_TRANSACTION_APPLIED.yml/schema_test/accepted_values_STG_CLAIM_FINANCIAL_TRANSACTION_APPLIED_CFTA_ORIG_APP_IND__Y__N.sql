
    
    




with all_values as (

    select distinct
        CFTA_ORIG_APP_IND as value_field

    from STAGING.STG_CLAIM_FINANCIAL_TRANSACTION_APPLIED

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'Y','N'
    )
)

select count(*) as validation_errors
from validation_errors


