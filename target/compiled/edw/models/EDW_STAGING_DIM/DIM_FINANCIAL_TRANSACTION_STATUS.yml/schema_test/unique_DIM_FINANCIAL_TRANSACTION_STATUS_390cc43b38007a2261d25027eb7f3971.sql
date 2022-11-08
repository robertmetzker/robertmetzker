
    
    



select count(*) as validation_errors
from (

    select
        FINANCIAL_TRANSACTION_STATUS_HKEY

    from EDW_STAGING_DIM.DIM_FINANCIAL_TRANSACTION_STATUS
    where FINANCIAL_TRANSACTION_STATUS_HKEY is not null
    group by FINANCIAL_TRANSACTION_STATUS_HKEY
    having count(*) > 1

) validation_errors


