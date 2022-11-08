
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_STATUS
where CLAIM_STATUS_HKEY is null


