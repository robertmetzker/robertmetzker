
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_EARNED_PREMIUM
where UNIQUE_ID_KEY is null


