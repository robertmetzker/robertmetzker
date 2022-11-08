
    
    




with all_values as (

    select distinct
        CUST_CNTC_DTL_TYP_NM as value_field

    from STAGING.STG_CUSTOMER_CONTACT_DETAIL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'EMAIL','FAX NUMBER','PHONE NUMBER','WEB SITE'
    )
)

select count(*) as validation_errors
from validation_errors


