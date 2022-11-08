
    
    



select count(*) as validation_errors
from (

    select
        UNIQUE_ID_KEY

    from STAGING.DST_FINANCIAL_TRANSACTION_STATUS
    where UNIQUE_ID_KEY is not null
    group by UNIQUE_ID_KEY
    having count(*) > 1

) validation_errors


