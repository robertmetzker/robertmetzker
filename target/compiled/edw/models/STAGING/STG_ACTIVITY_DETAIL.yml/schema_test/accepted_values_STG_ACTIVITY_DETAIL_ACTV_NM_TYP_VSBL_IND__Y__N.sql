
    
    




with all_values as (

    select distinct
        ACTV_NM_TYP_VSBL_IND as value_field

    from STAGING.STG_ACTIVITY_DETAIL

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


