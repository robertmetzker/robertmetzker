
    
    




with all_values as (

    select distinct
        PYRL_RPT_TYP_CD as value_field

    from STAGING.STG_PAYROLL_REPORT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'RPR'
    )
)

select count(*) as validation_errors
from validation_errors


