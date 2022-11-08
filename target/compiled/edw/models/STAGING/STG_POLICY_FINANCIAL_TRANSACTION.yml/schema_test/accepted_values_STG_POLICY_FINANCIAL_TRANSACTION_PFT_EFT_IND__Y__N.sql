
    
    




with all_values as (

    select distinct
        PFT_EFT_IND as value_field

    from STAGING.STG_POLICY_FINANCIAL_TRANSACTION

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


