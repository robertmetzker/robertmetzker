
    
    




with all_values as (

    select distinct
        CLM_TYP_CD as value_field

    from STAGING.STG_CLAIM_TYPE_HISTORY

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'LOSTTM','MEDONLY'
    )
)

select count(*) as validation_errors
from validation_errors


