
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_POLICY_FINANCIAL_TRANSACTION_COMMENT
where LOAD_DATETIME is null


