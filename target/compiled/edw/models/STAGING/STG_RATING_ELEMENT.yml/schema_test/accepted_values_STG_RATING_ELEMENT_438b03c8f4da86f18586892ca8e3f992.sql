
    
    




with all_values as (

    select distinct
        RT_ELEM_USAGE_TYP_CD as value_field

    from STAGING.STG_RATING_ELEMENT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'DISP','FCTR','PREM'
    )
)

select count(*) as validation_errors
from validation_errors


