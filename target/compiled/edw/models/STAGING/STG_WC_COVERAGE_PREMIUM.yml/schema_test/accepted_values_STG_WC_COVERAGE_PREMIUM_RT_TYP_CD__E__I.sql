
    
    




with all_values as (

    select distinct
        RT_TYP_CD as value_field

    from STAGING.STG_WC_COVERAGE_PREMIUM

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'E','I'
    )
)

select count(*) as validation_errors
from validation_errors


