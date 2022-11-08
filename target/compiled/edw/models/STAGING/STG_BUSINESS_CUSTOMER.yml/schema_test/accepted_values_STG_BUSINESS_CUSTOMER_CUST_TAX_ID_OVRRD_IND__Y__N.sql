
    
    




with all_values as (

    select distinct
        CUST_TAX_ID_OVRRD_IND as value_field

    from STAGING.STG_BUSINESS_CUSTOMER

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


