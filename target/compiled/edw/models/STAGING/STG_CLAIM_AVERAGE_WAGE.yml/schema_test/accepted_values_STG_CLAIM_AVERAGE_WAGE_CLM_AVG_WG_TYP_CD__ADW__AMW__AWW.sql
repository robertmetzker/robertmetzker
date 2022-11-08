
    
    




with all_values as (

    select distinct
        CLM_AVG_WG_TYP_CD as value_field

    from STAGING.STG_CLAIM_AVERAGE_WAGE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'ADW','AMW','AWW'
    )
)

select count(*) as validation_errors
from validation_errors


