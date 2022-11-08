
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT
where CLAIM_NUMBER is null


