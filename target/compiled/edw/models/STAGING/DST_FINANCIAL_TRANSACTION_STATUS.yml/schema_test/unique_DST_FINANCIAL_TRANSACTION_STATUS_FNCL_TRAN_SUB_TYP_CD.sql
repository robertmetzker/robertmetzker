
    
    



select count(*) as validation_errors
from (

    select
        FNCL_TRAN_SUB_TYP_CD

    from STAGING.DST_FINANCIAL_TRANSACTION_STATUS
    where FNCL_TRAN_SUB_TYP_CD is not null
    group by FNCL_TRAN_SUB_TYP_CD
    having count(*) > 1

) validation_errors


