
    
    



select count(*) as validation_errors
from (

    select
        CFTA_ID

    from STAGING.STG_CLAIM_FINANCIAL_TRANSACTION_APPLIED
    where CFTA_ID is not null
    group by CFTA_ID
    having count(*) > 1

) validation_errors


