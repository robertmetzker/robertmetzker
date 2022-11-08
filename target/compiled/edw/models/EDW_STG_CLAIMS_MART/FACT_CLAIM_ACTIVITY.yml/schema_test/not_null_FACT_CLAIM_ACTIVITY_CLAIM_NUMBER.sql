
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_ACTIVITY
where CLAIM_NUMBER is null


