
    
    




with all_values as (

    select distinct
        CLM_ICD_STS_PRI_IND as value_field

    from STAGING.STG_CLAIM_ICD_STATUS_TYPE

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


