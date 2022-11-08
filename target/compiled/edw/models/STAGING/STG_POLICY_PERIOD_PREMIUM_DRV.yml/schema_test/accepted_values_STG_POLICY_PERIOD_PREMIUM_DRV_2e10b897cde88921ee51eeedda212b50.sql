
    
    




with all_values as (

    select distinct
        PREM_TYP_NM as value_field

    from STAGING.STG_POLICY_PERIOD_PREMIUM_DRV

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'AUDITED','ESTIMATED','REPORTED'
    )
)

select count(*) as validation_errors
from validation_errors


