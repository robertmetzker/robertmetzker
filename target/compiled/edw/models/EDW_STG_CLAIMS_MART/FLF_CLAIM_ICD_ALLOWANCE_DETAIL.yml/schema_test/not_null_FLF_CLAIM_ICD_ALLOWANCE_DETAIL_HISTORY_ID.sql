
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FLF_CLAIM_ICD_ALLOWANCE_DETAIL
where HISTORY_ID is null

