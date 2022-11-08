
    
    




with all_values as (

    select distinct
        PLCY_AUDT_STS_TYP_CD as value_field

    from STAGING.STG_POLICY_AUDIT_STATUS_HISTORY

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'ASGN','COMP','RVS','RVWD','SEL','VOID'
    )
)

select count(*) as validation_errors
from validation_errors


