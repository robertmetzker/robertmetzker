
    
    




with all_values as (

    select distinct
        CLM_AVG_WG_TYP_NM as value_field

    from STAGING.STG_CLAIM_AVERAGE_WAGE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'DAILY','MONTHLY','WEEKLY'
    )
)

select count(*) as validation_errors
from validation_errors


