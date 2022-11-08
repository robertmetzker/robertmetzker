
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_POLICY_STANDING
where POLICY_STANDING_HKEY is null


