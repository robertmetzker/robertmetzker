
    
    



select count(*) as validation_errors
from (

    select
        FINANCIAL_TRANSACTION_TYPE_ID

    from STAGING.DSV_FINANCIAL_TRANSACTION_TYPE
    where FINANCIAL_TRANSACTION_TYPE_ID is not null
    group by FINANCIAL_TRANSACTION_TYPE_ID
    having count(*) > 1

) validation_errors


