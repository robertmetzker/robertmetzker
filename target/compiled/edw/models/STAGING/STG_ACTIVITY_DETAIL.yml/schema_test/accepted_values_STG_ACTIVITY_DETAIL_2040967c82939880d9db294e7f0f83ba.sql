
    
    




with all_values as (

    select distinct
        ACTV_ACTN_TYP_NM as value_field

    from STAGING.STG_ACTIVITY_DETAIL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'ADDED','VOIDED','UPDATED'
    )
)

select count(*) as validation_errors
from validation_errors


