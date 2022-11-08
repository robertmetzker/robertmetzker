
    
    




with all_values as (

    select distinct
        INDM_FREQ_TYP_NM as value_field

    from STAGING.STG_INDEMNITY_SCHEDULE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        '7 DAYS','14 DAYS','21 DAYS','28 DAYS','BI-MONTHLY','MONTHLY FIXED','LUMP SUM','MONTHLY','QUARTERLY'
    )
)

select count(*) as validation_errors
from validation_errors


