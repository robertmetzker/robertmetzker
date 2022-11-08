
    
    



select count(*) as validation_errors
from (

    select
        FINANCIAL_TRANSACTION_TYPE_HKEY

    from EDW_STAGING_DIM.DIM_FINANCIAL_TRANSACTION_TYPE
    where FINANCIAL_TRANSACTION_TYPE_HKEY is not null
    group by FINANCIAL_TRANSACTION_TYPE_HKEY
    having count(*) > 1

) validation_errors


