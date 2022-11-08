
    
    



select count(*) as validation_errors
from STAGING.DSV_FINANCIAL_TRANSACTION_TYPE
where FINANCIAL_TRANSACTION_TYPE_ID is null


