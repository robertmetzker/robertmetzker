
    
    




with all_values as (

    select distinct
        CLM_WG_RPT_SRC_TYP_CD as value_field

    from STAGING.STG_CLAIM_WAGE_SOURCE_DETAIL

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'INDCMSNDECSN','SIEMPRPTD','WGLOSSEARN','WGOVRRD','WGSETWKSHT'
    )
)

select count(*) as validation_errors
from validation_errors


