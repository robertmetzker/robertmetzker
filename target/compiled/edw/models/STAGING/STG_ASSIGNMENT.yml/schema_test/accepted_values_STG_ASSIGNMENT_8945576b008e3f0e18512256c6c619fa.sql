
    
    




with all_values as (

    select distinct
        ASGN_STS_TYP_NM as value_field

    from STAGING.STG_ASSIGNMENT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'ASSIGNMENT','PENDING REASSIGNMENT','REASSIGNMENT'
    )
)

select count(*) as validation_errors
from validation_errors


