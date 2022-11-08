
    
    



select count(*) as validation_errors
from STAGING.DST_POLICY_FINANCIAL_TRANSACTION_COMMENT
where UNIQUE_ID_KEY is null


