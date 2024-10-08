
    
    




with all_values as (

    select distinct
        PLCY_PRD_BILL_SCH_TRU_UP_IND as value_field

    from STAGING.STG_POLICY_PERIOD_BILLING_SCHEDULE

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


