
    
    




with all_values as (

    select distinct
        APP_CNTX_TYP_CD as value_field

    from STAGING.STG_ASSIGNMENT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'ADDRESS','BATCH','CASE','CLAIM','CONTT','CUSTOMER','DOCUMENT','EXTINTF','FINANCIALS','PAY_REQS','POLICY','QUOTE','SYS_CONSTANT'
    )
)

select count(*) as validation_errors
from validation_errors


