
    
    




with all_values as (

    select distinct
        TAX_ID_USE_FOR_1099_IND as value_field

    from STAGING.STG_TAX_IDENTIFIER

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


