
    
    




with all_values as (

    select distinct
        CNTX_TYP_NM as value_field

    from STAGING.STG_ACTIVITY

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'BILL','BLOCK','BUREAU','CASE','CLAIM','CLAIM PARTICIPATION','CLAIM RESERVES','CUSTOMER','CUSTOMER ROLE','FINANCIAL','IDENTIFIER','POLICY BLOCK','POLICY PERIOD','TASK'
    )
)

select count(*) as validation_errors
from validation_errors


