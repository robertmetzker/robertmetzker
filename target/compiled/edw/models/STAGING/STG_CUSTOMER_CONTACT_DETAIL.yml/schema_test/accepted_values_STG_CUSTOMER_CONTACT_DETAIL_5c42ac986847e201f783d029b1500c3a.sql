
    
    




with all_values as (

    select distinct
        PHN_NO_TYP_NM as value_field

    from STAGING.STG_CUSTOMER_CONTACT_DETAIL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'BUSINESS','BUSINESS DIRECT DIAL','CELL','FAX','HOME','OTHER','TTY/TDD'
    )
)

select count(*) as validation_errors
from validation_errors


