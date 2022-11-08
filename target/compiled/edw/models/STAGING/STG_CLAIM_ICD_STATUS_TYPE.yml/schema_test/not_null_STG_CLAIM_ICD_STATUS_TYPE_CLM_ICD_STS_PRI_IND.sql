
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_ICD_STATUS_TYPE
where CLM_ICD_STS_PRI_IND is null


