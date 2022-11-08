
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_INVESTIGATION
where CLAIM_INVESTIGATION_HKEY is null


