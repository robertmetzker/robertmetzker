
    
    




with all_values as (

    select distinct
        CUST_CNTC_DTL_SUB_TYP as value_field

    from STAGING.STG_CUSTOMER_CONTACT_DETAIL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'B','BDD','C','F','H','O','T'
    )
)

select count(*) as validation_errors
from validation_errors


