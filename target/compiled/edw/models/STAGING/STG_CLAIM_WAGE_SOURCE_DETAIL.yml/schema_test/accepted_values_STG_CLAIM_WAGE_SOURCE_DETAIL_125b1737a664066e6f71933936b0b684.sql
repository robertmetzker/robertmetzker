
    
    




with all_values as (

    select distinct
        CLM_PAY_PRD_TYP_CD as value_field

    from STAGING.STG_CLAIM_WAGE_SOURCE_DETAIL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'BIWEK','DAILY','HR','MMY','OTH','SEMIMMY','WEK','YR'
    )
)

select count(*) as validation_errors
from validation_errors


