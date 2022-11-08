
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM
where CLAIM_HKEY is null


