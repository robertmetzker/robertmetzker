
    
    




with all_values as (

    select distinct
        RT_TYP_NM as value_field

    from STAGING.STG_WC_COVERAGE_PREMIUM

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'EMPLOYEE','INCLUDED INDIVIDUAL'
    )
)

select count(*) as validation_errors
from validation_errors


