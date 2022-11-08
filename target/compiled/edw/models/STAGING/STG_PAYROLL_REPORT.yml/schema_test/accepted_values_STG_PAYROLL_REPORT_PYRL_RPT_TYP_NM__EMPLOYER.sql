
    
    




with all_values as (

    select distinct
        PYRL_RPT_TYP_NM as value_field

    from STAGING.STG_PAYROLL_REPORT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'EMPLOYER'
    )
)

select count(*) as validation_errors
from validation_errors


