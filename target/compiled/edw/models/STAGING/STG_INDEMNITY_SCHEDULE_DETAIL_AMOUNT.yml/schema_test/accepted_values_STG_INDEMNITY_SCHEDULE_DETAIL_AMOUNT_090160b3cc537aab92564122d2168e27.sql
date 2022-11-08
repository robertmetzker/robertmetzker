
    
    




with all_values as (

    select distinct
        INDM_SCH_DTL_AMT_TYP_NM as value_field

    from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'BENEFIT','FINANCIAL ADJUSTMENT','OFFSET','OFFSET ADJUSTMENT'
    )
)

select count(*) as validation_errors
from validation_errors


