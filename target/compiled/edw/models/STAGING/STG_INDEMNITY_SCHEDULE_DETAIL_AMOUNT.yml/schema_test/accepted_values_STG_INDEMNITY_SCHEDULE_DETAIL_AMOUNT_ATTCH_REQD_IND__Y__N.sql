
    
    




with all_values as (

    select distinct
        ATTCH_REQD_IND as value_field

    from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT

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


