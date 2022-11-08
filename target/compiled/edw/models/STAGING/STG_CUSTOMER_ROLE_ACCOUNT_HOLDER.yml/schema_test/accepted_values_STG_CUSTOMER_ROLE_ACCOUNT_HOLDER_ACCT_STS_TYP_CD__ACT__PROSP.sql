
    
    




with all_values as (

    select distinct
        ACCT_STS_TYP_CD as value_field

    from STAGING.STG_CUSTOMER_ROLE_ACCOUNT_HOLDER

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'ACT','PROSP'
    )
)

select count(*) as validation_errors
from validation_errors


