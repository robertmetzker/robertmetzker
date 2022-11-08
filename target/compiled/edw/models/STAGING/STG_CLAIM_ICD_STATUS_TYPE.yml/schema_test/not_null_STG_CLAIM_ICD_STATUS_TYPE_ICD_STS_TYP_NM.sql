
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_ICD_STATUS_TYPE
where ICD_STS_TYP_NM is null


