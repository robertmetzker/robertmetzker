
    
    




with all_values as (

    select distinct
        CNTX_TYP_CD as value_field

    from STAGING.STG_ACTIVITY

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'BILL','BLK','BUR','CASE','CLM','CLM_PTCP','CLM_RSRV','CUST','CUST_ROLE','FINANCIAL','IDENTIFIER','PLCY_BLK','PLCY_PRD','TASK'
    )
)

select count(*) as validation_errors
from validation_errors


