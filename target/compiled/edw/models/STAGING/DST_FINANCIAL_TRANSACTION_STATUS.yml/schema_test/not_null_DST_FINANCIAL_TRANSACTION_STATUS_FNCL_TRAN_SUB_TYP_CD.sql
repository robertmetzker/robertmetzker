
    
    



select count(*) as validation_errors
from STAGING.DST_FINANCIAL_TRANSACTION_STATUS
where FNCL_TRAN_SUB_TYP_CD is null

