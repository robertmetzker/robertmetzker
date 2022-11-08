
    
    




with all_values as (

    select distinct
        INTRN_CHNL_TYP_CD as value_field

    from STAGING.STG_CUSTOMER_INTERACTION_CHANNEL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'E','F','P','W'
    )
)

select count(*) as validation_errors
from validation_errors


