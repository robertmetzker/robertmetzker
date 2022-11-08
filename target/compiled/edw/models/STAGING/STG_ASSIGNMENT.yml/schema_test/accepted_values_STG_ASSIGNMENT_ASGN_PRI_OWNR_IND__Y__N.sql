
    
    




with all_values as (

    select distinct
        ASGN_PRI_OWNR_IND as value_field

    from STAGING.STG_ASSIGNMENT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'Y','N'
    )
)

select count(*) as validation_errors
from validation_errors


