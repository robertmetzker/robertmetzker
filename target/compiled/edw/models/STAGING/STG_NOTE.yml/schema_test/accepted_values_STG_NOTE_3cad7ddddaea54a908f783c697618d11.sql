
    
    




with all_values as (

    select distinct
        NOTE_CALL_TYP_NM as value_field

    from STAGING.STG_NOTE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'CALL PLACED','CALL RECEIVED'
    )
)

select count(*) as validation_errors
from validation_errors


