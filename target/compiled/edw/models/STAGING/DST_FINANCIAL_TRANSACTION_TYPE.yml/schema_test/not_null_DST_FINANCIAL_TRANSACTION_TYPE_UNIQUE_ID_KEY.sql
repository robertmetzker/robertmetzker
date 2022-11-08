
    
    



select count(*) as validation_errors
from STAGING.DST_FINANCIAL_TRANSACTION_TYPE
where UNIQUE_ID_KEY is null


