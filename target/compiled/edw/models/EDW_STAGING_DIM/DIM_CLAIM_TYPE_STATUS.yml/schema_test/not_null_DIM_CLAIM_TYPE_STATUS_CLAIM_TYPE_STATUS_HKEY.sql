
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_TYPE_STATUS
where CLAIM_TYPE_STATUS_HKEY is null


