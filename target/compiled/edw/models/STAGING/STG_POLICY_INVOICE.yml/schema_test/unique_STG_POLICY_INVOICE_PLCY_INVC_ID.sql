
    
    



select count(*) as validation_errors
from (

    select
        PLCY_INVC_ID

    from STAGING.STG_POLICY_INVOICE
    where PLCY_INVC_ID is not null
    group by PLCY_INVC_ID
    having count(*) > 1

) validation_errors


