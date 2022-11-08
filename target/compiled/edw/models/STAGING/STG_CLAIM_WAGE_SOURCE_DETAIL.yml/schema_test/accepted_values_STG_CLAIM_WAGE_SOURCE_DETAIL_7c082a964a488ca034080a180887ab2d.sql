
    
    




with all_values as (

    select distinct
        CLM_WG_TYP_NM as value_field

    from STAGING.STG_CLAIM_WAGE_SOURCE_DETAIL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'AWW','FWW','PENDING AWW','PENDING FWW','POST INJURY WAGES'
    )
)

select count(*) as validation_errors
from validation_errors


