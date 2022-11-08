
    
    



select count(*) as validation_errors
from (

    select
        FNCL_TRAN_TYP_ID

    from STAGING.DST_FINANCIAL_TRANSACTION_TYPE
    where FNCL_TRAN_TYP_ID is not null
    group by FNCL_TRAN_TYP_ID
    having count(*) > 1

) validation_errors


