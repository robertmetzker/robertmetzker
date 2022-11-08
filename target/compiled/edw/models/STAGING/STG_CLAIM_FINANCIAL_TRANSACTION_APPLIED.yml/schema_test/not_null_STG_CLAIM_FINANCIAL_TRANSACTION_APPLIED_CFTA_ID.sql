
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_FINANCIAL_TRANSACTION_APPLIED
where CFTA_ID is null


