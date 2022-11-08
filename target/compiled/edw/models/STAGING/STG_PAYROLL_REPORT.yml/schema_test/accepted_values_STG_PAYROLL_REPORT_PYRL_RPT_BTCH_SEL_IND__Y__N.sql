
    
    




with all_values as (

    select distinct
        PYRL_RPT_BTCH_SEL_IND as value_field

    from STAGING.STG_PAYROLL_REPORT

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


