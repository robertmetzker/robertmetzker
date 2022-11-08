
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_FINANCIAL_TRANSACTION_APPLIED
where FTAT_ID is null


