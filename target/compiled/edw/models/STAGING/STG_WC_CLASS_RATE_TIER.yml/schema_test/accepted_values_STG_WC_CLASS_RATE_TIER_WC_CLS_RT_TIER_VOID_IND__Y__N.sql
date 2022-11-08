
    
    




with all_values as (

    select distinct
        WC_CLS_RT_TIER_VOID_IND as value_field

    from STAGING.STG_WC_CLASS_RATE_TIER

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


