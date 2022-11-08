
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_FINANCIAL_TRANSACTION
where CLM_NO is null


