
    
    




with all_values as (

    select distinct
        INDM_SCH_DTL_STS_TYP_CD as value_field

    from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'HOLD','PAID','PND','PNDAUTH','PNDTRNSFR','REL'
    )
)

select count(*) as validation_errors
from validation_errors


