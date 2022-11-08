
    
    




with all_values as (

    select distinct
        INDM_SCH_DTL_STS_TYP_NM as value_field

    from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'ON HOLD','PAID','PENDING','PENDING AUTHORIZATION','PENDING TRANSFER','RELEASED'
    )
)

select count(*) as validation_errors
from validation_errors


