
    
    



select count(*) as validation_errors
from (

    select
        FINANCIAL_TRANSACTION_STATUS_CODE

    from STAGING.DSV_FINANCIAL_TRANSACTION_STATUS
    where FINANCIAL_TRANSACTION_STATUS_CODE is not null
    group by FINANCIAL_TRANSACTION_STATUS_CODE
    having count(*) > 1

) validation_errors


