
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FLF_CLAIM_ICD_HISTORY
where CLAIM_INITAL_FILE_DATE_KEY is null


