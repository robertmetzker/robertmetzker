
    
    




with all_values as (

    select distinct
        PLCY_INVC_TYP_NM as value_field

    from STAGING.STG_POLICY_INVOICE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'POLICY INVOICE','ACCOUNT INVOICE','DEDUCTIBLE INVOICE'
    )
)

select count(*) as validation_errors
from validation_errors


