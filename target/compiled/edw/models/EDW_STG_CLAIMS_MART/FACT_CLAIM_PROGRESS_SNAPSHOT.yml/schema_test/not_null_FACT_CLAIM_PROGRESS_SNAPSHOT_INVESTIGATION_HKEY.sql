
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_PROGRESS_SNAPSHOT
where INVESTIGATION_HKEY is null


