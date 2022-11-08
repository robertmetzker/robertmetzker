
    
    



select count(*) as validation_errors
from STAGING.DST_FINANCIAL_TRANSACTION_STATUS
where UNIQUE_ID_KEY is null


