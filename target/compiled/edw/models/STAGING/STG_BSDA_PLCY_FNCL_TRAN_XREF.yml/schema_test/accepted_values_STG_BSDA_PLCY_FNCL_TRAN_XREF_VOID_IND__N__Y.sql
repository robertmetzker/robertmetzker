
    
    




with all_values as (

    select distinct
        VOID_IND as value_field

    from STAGING.STG_BSDA_PLCY_FNCL_TRAN_XREF

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'N','Y'
    )
)

select count(*) as validation_errors
from validation_errors


