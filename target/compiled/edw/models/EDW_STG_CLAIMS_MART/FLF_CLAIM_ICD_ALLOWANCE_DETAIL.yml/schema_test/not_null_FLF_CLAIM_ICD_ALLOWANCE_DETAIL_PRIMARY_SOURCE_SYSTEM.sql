
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FLF_CLAIM_ICD_ALLOWANCE_DETAIL
where PRIMARY_SOURCE_SYSTEM is null


