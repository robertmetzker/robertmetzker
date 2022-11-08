
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_ICD_STATUS_TYPE
where CLAIM_ICD_STATUS_TYPE_HKEY is null


