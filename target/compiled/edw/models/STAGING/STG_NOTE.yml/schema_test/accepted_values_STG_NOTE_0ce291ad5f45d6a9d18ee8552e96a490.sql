
    
    




with all_values as (

    select distinct
        NOTE_TYP_CD as value_field

    from STAGING.STG_NOTE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'NOTE','PHONE','CORRESPONDENCE','SMS'
    )
)

select count(*) as validation_errors
from validation_errors


