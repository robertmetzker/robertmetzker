
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_ACCIDENT_DESCRIPTION
where LOAD_DATETIME is null


