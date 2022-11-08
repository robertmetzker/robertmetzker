
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_FINANCIAL_TRANSACTION_TYPE
where LOAD_DATETIME is null


