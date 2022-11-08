
    
    



select count(*) as validation_errors
from STAGING.DSV_FINANCIAL_TRANSACTION_STATUS
where FINANCIAL_TRANSACTION_STATUS_CODE is null


