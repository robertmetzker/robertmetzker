
    
    



select count(*) as validation_errors
from STAGING.DSV_EARNED_PREMIUM
where FINANCIAL_TRANSACTION_ID is null


