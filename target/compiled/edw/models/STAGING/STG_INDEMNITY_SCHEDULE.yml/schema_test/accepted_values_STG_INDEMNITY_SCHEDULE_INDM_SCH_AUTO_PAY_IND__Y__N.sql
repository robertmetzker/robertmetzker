
    
    




with all_values as (

    select distinct
        INDM_SCH_AUTO_PAY_IND as value_field

    from STAGING.STG_INDEMNITY_SCHEDULE

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

