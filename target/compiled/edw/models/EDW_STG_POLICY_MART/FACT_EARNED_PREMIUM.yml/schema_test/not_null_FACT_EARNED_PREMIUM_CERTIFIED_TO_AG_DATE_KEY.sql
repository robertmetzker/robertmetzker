
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_EARNED_PREMIUM
where CERTIFIED_TO_AG_DATE_KEY is null


