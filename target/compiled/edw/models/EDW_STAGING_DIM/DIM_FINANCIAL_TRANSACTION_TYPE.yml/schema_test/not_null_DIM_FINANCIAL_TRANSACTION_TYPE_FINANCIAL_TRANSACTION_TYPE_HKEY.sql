
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_FINANCIAL_TRANSACTION_TYPE
where FINANCIAL_TRANSACTION_TYPE_HKEY is null


