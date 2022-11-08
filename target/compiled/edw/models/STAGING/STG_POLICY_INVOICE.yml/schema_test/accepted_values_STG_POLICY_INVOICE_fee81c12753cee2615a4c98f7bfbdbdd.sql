
    
    




with all_values as (

    select distinct
        PLCY_INVC_TYP_CD as value_field

    from STAGING.STG_POLICY_INVOICE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'PLCY_INVC','ACCT_INVC','DPL_INVC'
    )
)

select count(*) as validation_errors
from validation_errors


