
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_STATUS
where LOAD_DATETIME is null


