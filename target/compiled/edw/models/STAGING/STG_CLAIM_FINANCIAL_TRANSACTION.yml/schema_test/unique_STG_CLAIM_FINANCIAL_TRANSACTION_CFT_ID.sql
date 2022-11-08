
    
    



select count(*) as validation_errors
from (

    select
        CFT_ID

    from STAGING.STG_CLAIM_FINANCIAL_TRANSACTION
    where CFT_ID is not null
    group by CFT_ID
    having count(*) > 1

) validation_errors


