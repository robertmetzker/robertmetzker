
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_PROGRESS_SNAPSHOT
where LAST_DAY_AT_WORK_DATE_KEY is null

