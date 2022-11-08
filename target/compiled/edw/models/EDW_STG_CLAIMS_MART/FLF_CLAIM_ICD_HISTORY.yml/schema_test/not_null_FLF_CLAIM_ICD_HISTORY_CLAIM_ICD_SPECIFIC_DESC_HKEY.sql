
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FLF_CLAIM_ICD_HISTORY
where CLAIM_ICD_SPECIFIC_DESC_HKEY is null


