
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_ICD_STATUS_DETAIL
where CLAIM_ICD_STATUS_DETAIL_HKEY is null


