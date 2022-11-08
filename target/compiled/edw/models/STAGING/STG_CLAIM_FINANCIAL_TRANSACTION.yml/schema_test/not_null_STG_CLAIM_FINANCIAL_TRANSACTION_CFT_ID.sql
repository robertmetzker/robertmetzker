
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_FINANCIAL_TRANSACTION
where CFT_ID is null


