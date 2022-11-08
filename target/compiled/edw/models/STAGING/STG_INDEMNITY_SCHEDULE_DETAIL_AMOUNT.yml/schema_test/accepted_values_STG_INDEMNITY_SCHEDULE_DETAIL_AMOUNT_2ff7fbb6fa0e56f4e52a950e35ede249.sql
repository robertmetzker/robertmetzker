
    
    




with all_values as (

    select distinct
        INDM_SCH_DTL_AMT_TYP_CD as value_field

    from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'BNFT','FNCLADJ','OFST','OFSTADJ'
    )
)

select count(*) as validation_errors
from validation_errors


