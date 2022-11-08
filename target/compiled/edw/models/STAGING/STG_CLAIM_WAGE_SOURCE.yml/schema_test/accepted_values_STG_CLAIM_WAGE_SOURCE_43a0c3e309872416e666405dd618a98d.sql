
    
    




with all_values as (

    select distinct
        CLM_EMPL_STS_TYP_CD as value_field

    from STAGING.STG_CLAIM_WAGE_SOURCE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'FULLTIME','INJ_DUR_REH','NOT_EMPL','OTH','PARTTIME','PC_WK','RTRD','SEASONAL','SEP','VOLNR'
    )
)

select count(*) as validation_errors
from validation_errors


