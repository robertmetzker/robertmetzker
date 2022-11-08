
    
    




with all_values as (

    select distinct
        PREM_BS_TYP_CD as value_field

    from STAGING.STG_WC_CLASS

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'PER_CPTA','TOT_REMN'
    )
)

select count(*) as validation_errors
from validation_errors


