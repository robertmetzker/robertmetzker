
    
    




with all_values as (

    select distinct
        CAWCST_CD as value_field

    from STAGING.STG_CLAIM_AVERAGE_WAGE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'CALC','NOTCALC','RECALC'
    )
)

select count(*) as validation_errors
from validation_errors


