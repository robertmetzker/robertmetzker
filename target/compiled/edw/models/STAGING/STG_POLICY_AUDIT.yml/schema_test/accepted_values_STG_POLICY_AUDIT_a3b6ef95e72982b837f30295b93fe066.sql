
    
    




with all_values as (

    select distinct
        PLCY_AUDT_TYP_CD as value_field

    from STAGING.STG_POLICY_AUDIT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'EST','ETU','FLD','TU'
    )
)

select count(*) as validation_errors
from validation_errors


