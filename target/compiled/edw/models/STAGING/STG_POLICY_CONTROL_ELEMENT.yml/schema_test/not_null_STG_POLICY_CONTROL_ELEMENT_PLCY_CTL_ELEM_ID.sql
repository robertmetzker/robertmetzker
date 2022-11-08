
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_CONTROL_ELEMENT
where PLCY_CTL_ELEM_ID is null


