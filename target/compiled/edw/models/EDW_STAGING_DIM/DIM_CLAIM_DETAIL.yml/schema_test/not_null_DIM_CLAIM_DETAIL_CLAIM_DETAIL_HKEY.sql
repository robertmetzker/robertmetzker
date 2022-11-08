
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_DETAIL
where CLAIM_DETAIL_HKEY is null


