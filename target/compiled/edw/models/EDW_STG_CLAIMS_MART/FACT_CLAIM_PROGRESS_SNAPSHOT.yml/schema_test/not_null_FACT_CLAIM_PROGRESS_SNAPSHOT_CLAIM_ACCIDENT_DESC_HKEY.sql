
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_PROGRESS_SNAPSHOT
where CLAIM_ACCIDENT_DESC_HKEY is null


