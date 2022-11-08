
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FLF_CLAIM_ICD_HISTORY
where ICD_HKEY is null


