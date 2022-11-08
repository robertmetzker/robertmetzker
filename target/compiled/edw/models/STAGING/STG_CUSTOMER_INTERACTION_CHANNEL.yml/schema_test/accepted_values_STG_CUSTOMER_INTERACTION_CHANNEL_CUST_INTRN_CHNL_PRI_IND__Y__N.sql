
    
    




with all_values as (

    select distinct
        CUST_INTRN_CHNL_PRI_IND as value_field

    from STAGING.STG_CUSTOMER_INTERACTION_CHANNEL

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


