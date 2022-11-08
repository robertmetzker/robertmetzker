
    
    




with all_values as (

    select distinct
        WH_PRD_TYP_NM as value_field

    from STAGING.STG_CUSTOMER_CHILD_SUPPORT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'BIWEEKLY','DAILY','MONTHLY','SEMI-MONTHLY','WEEKLY','YEARLY'
    )
)

select count(*) as validation_errors
from validation_errors


