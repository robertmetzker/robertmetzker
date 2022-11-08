
    
    




with all_values as (

    select distinct
        CAWCST_NM as value_field

    from STAGING.STG_CLAIM_AVERAGE_WAGE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'CALCULATED','NOT CALCULATED','RE-CALCULATE'
    )
)

select count(*) as validation_errors
from validation_errors


