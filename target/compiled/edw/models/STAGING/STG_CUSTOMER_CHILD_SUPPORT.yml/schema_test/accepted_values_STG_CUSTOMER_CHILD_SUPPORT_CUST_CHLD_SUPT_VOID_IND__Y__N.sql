
    
    




with all_values as (

    select distinct
        CUST_CHLD_SUPT_VOID_IND as value_field

    from STAGING.STG_CUSTOMER_CHILD_SUPPORT

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


