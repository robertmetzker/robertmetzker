
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_POLICY_PERIOD_STATUS
where POLICY_STATUS_TRANSACTION_DATE_KEY is null


