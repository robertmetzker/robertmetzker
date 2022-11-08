
    
    




with all_values as (

    select distinct
        RT_ELEM_USAGE_TYP_DESC as value_field

    from STAGING.STG_RATING_ELEMENT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'DISPLAY','FACTOR','PREMIUM'
    )
)

select count(*) as validation_errors
from validation_errors


