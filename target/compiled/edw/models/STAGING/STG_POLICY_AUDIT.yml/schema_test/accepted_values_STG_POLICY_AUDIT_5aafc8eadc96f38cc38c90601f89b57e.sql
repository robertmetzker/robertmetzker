
    
    




with all_values as (

    select distinct
        PAUAST_CD as value_field

    from STAGING.STG_POLICY_AUDIT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'APRV','COMP','PND_UW_RVW','REJ'
    )
)

select count(*) as validation_errors
from validation_errors


