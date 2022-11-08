
    
    




with all_values as (

    select distinct
        CAWWSD_INCL_IND as value_field

    from STAGING.STG_CLAIM_AVERAGE_WAGE_SOURCE_DETAIL

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


