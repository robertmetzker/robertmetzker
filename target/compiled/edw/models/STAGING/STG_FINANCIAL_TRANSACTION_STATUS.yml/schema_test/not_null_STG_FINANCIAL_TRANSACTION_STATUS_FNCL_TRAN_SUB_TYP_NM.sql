
    
    



select count(*) as validation_errors
from STAGING.STG_FINANCIAL_TRANSACTION_STATUS
where FNCL_TRAN_SUB_TYP_NM is null


