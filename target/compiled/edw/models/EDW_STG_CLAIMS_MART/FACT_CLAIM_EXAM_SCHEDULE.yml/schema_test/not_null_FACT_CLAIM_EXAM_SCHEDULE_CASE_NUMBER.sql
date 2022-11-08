
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE
where CASE_NUMBER is null


