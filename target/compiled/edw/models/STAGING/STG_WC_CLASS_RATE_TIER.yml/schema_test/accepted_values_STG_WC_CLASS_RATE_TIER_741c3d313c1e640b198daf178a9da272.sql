
    
    




with all_values as (

    select distinct
        WC_CLS_RT_TIER_TYP_CD as value_field

    from STAGING.STG_WC_CLASS_RATE_TIER

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'RT_1','RT_2'
    )
)

select count(*) as validation_errors
from validation_errors


