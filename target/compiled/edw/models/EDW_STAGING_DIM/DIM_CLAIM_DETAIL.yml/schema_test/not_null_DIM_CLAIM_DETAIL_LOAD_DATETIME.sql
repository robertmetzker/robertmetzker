
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_DETAIL
where LOAD_DATETIME is null


