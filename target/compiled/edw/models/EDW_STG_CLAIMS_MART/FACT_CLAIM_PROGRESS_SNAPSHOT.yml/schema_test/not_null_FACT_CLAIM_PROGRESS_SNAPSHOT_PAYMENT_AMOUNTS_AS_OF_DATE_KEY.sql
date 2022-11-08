
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_PROGRESS_SNAPSHOT
where PAYMENT_AMOUNTS_AS_OF_DATE_KEY is null


