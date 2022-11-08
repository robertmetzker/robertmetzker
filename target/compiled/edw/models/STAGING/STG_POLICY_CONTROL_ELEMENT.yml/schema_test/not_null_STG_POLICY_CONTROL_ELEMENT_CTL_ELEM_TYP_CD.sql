
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_CONTROL_ELEMENT
where CTL_ELEM_TYP_CD is null


