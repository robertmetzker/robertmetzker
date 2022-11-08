
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_FINANCIAL_TRANSACTION_APPLIED
where FTAT_ID is null


