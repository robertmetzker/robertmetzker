
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_EARNED_PREMIUM
where FINANCIAL_TRANSACTION_CREATE_USER_HKEY is null


