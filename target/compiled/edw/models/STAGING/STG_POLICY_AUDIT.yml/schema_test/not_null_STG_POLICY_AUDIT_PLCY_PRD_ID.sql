
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_AUDIT
where PLCY_PRD_ID is null


