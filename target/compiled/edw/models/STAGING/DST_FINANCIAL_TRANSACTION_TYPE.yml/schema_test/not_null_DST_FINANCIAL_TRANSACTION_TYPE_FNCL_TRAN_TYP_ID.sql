
    
    



select count(*) as validation_errors
from STAGING.DST_FINANCIAL_TRANSACTION_TYPE
where FNCL_TRAN_TYP_ID is null


