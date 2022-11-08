
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_ICD_STATUS
where CLM_ICD_STS_ID is null


