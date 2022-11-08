
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_POLICY_PERIOD
where POLICY_PERIOD_HKEY is null


