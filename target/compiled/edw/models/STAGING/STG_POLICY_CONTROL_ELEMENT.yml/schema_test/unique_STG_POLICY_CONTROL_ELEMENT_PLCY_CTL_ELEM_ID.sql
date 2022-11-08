
    
    



select count(*) as validation_errors
from (

    select
        PLCY_CTL_ELEM_ID

    from STAGING.STG_POLICY_CONTROL_ELEMENT
    where PLCY_CTL_ELEM_ID is not null
    group by PLCY_CTL_ELEM_ID
    having count(*) > 1

) validation_errors


