
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_ICD_SPECIFIC_DESC
where LOAD_DATETIME is null


