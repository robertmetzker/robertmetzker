
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_FINANCIAL_TRANSACTION_APPLIED
where PFTA_ID is null


