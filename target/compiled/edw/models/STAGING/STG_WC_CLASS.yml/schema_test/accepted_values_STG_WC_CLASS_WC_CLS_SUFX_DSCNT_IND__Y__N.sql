
    
    




with all_values as (

    select distinct
        WC_CLS_SUFX_DSCNT_IND as value_field

    from STAGING.STG_WC_CLASS

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


