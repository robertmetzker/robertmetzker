
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_FINANCIAL_TRANSACTION_APPLIED
where AUDIT_USER_ID_CREA is null


