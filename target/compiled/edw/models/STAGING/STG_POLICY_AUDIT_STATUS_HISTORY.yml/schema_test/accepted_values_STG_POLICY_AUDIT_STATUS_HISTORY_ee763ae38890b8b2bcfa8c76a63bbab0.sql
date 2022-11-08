
    
    




with all_values as (

    select distinct
        PLCY_AUDT_STS_TYP_NM as value_field

    from STAGING.STG_POLICY_AUDIT_STATUS_HISTORY

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'ASSIGNED','COMPLETED','REVISED','REVIEWED','SELECTED','VOID'
    )
)

select count(*) as validation_errors
from validation_errors


