
    
    




with all_values as (

    select distinct
        INTRN_CHNL_TYP_NM as value_field

    from STAGING.STG_CUSTOMER_INTERACTION_CHANNEL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'E-MAIL','FTP SITE','PHONE','WEBSITE'
    )
)

select count(*) as validation_errors
from validation_errors


