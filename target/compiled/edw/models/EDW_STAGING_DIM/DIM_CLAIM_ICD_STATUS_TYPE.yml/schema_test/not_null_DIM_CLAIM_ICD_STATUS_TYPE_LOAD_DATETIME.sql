
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_ICD_STATUS_TYPE
where LOAD_DATETIME is null


