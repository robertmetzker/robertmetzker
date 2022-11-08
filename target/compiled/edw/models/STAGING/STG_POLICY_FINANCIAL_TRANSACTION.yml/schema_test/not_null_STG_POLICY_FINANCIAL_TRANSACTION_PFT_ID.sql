
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_FINANCIAL_TRANSACTION
where PFT_ID is null


