
    
    




with all_values as (

    select distinct
        TAX_ID_TYP_NM as value_field

    from STAGING.STG_TAX_IDENTIFIER

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'BN','CONVERTED DUPLICATE SSN','EMPLOYMENT VISA NUMBER','FEIN','GREEN CARD NUMBER','PASSPORT NUMBER','SIN','SSN'
    )
)

select count(*) as validation_errors
from validation_errors


