
    
    




with all_values as (

    select distinct
        INDM_SCH_DTL_AMT_JNT_PAYE_IND as value_field

    from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT

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


