
    
    




with all_values as (

    select distinct
        BILL_SCH_DTL_ADV_BTCH_IND as value_field

    from STAGING.STG_BILLING_SCHEDULE_DETAIL

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


