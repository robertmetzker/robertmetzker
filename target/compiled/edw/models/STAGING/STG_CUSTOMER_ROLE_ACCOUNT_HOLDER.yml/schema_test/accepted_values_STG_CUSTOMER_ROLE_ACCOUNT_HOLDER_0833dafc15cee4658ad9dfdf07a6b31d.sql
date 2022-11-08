
    
    




with all_values as (

    select distinct
        ACCT_STS_TYP_NM as value_field

    from STAGING.STG_CUSTOMER_ROLE_ACCOUNT_HOLDER

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'ACTIVE','PROSPECT'
    )
)

select count(*) as validation_errors
from validation_errors


