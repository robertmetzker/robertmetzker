
    
    



select count(*) as validation_errors
from STAGING.STG_FINANCIAL_TRANSACTION_TYPE
where FNCL_TRAN_TYP_CD is null


