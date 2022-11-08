
    
    




with all_values as (

    select distinct
        CLM_WG_CTG_TYP_NM as value_field

    from STAGING.STG_CLAIM_WAGE_SOURCE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'POST INJURY','PRE-INJURY','RECURRENT INJURY'
    )
)

select count(*) as validation_errors
from validation_errors


