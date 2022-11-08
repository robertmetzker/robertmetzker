
    
    




with all_values as (

    select distinct
        PYRL_RPT_STS_TYP_NM as value_field

    from STAGING.STG_PAYROLL_REPORT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'OPEN','PENDING','COMPLETED'
    )
)

select count(*) as validation_errors
from validation_errors


