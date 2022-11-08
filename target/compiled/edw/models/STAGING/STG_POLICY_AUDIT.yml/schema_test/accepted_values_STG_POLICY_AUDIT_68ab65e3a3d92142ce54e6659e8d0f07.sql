
    
    




with all_values as (

    select distinct
        AUDT_PRCS_TYP_NM as value_field

    from STAGING.STG_POLICY_AUDIT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'ACTUAL EXPOSURE','CLOSE EVEN','ESTIMATED EXPOSURE','PRO-RATE TO PREMIUM FINANCE COMPANY'
    )
)

select count(*) as validation_errors
from validation_errors


