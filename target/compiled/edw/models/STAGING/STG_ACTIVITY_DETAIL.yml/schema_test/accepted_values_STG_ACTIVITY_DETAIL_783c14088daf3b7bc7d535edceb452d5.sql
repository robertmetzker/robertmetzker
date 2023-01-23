
    
    




with all_values as (

    select distinct
        ACTV_ACTN_TYP_CD as value_field

    from STAGING.STG_ACTIVITY_DETAIL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'CREATE','DELETE','UPDATE'
    )
)

select count(*) as validation_errors
from validation_errors

