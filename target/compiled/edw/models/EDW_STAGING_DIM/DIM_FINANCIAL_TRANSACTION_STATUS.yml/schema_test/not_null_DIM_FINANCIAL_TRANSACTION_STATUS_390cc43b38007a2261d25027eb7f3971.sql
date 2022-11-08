
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_FINANCIAL_TRANSACTION_STATUS
where FINANCIAL_TRANSACTION_STATUS_HKEY is null


