
    
    




with all_values as (

    select distinct
        INDM_FREQ_TYP_CD as value_field

    from STAGING.STG_INDEMNITY_SCHEDULE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        '7','14','21','28','BIMNTHLY','FIXED','LMP','MNTHLY','QTRLY'
    )
)

select count(*) as validation_errors
from validation_errors


