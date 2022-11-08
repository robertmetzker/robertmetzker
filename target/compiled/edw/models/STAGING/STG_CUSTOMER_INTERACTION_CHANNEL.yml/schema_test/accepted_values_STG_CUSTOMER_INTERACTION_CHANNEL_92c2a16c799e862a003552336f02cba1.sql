
    
    




with all_values as (

    select distinct
        PHN_NO_TYP_CD as value_field

    from STAGING.STG_CUSTOMER_INTERACTION_CHANNEL

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


