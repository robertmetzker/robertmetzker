
    
    




with all_values as (

    select distinct
        ASGN_STS_TYP_CD as value_field

    from STAGING.STG_ASSIGNMENT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'ASGN','PND_REASGN','REASGN'
    )
)

select count(*) as validation_errors
from validation_errors


