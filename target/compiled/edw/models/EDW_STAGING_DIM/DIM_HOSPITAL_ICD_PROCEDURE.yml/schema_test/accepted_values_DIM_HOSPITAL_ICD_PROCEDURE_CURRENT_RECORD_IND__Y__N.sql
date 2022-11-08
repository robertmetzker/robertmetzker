
    
    




with all_values as (

    select distinct
        CURRENT_RECORD_IND as value_field

    from EDW_STAGING_DIM.DIM_HOSPITAL_ICD_PROCEDURE

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


