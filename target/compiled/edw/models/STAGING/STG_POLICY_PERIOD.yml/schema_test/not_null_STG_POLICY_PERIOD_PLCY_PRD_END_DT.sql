
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_PERIOD
where PLCY_PRD_END_DT is null


