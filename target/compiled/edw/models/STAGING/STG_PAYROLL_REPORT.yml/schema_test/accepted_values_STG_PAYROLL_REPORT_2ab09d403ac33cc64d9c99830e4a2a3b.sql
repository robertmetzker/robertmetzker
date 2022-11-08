
    
    




with all_values as (

    select distinct
        PYRL_RPT_STS_TYP_CD as value_field

    from STAGING.STG_PAYROLL_REPORT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'OP','PEN','COM'
    )
)

select count(*) as validation_errors
from validation_errors


