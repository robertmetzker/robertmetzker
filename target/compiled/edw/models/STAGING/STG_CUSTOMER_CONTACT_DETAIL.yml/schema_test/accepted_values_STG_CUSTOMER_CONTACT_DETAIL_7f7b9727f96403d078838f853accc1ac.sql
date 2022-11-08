
    
    




with all_values as (

    select distinct
        CUST_CNTC_DTL_TYP_CD as value_field

    from STAGING.STG_CUSTOMER_CONTACT_DETAIL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'EML','FAX','PHN','WEB'
    )
)

select count(*) as validation_errors
from validation_errors


