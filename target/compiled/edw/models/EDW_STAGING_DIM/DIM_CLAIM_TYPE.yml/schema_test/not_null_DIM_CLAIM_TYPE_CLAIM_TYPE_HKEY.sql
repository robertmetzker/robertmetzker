
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_TYPE
where CLAIM_TYPE_HKEY is null


