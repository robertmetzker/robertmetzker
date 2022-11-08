
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_PROGRESS_SNAPSHOT
where ACTUAL_RETURN_TO_WORK_DATE_KEY is null


