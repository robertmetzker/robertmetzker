
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_ICD_DESC
where UNIQUE_ID_KEY is null


