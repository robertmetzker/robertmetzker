
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE
where EXAM_REPORT_RECEIVED_DATE_KEY is null


