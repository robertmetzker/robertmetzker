
    
    




with all_values as (

    select distinct
        PLCY_PRFL_CTG_TYP_NM as value_field

    from STAGING.STG_POLICY_PROFILE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'ATTRIBUTES','QUESTIONS'
    )
)

select count(*) as validation_errors
from validation_errors


