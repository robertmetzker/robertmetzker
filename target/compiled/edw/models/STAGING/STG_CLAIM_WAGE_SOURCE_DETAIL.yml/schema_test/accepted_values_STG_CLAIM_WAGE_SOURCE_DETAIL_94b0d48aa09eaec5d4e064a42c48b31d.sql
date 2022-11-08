
    
    




with all_values as (

    select distinct
        CLM_WG_RPT_SRC_TYP_NM as value_field

    from STAGING.STG_CLAIM_WAGE_SOURCE_DETAIL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'INDUSTRIAL COMMISSION DECISION','SI EMPLOYER REPORTED','WAGE LOSS EARNINGS','WAGE OVERRIDE','WAGE SETTING WORKSHEET'
    )
)

select count(*) as validation_errors
from validation_errors


