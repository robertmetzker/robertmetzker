
    
    



select count(*) as validation_errors
from (

    select
        PFT_ID

    from STAGING.STG_POLICY_FINANCIAL_TRANSACTION
    where PFT_ID is not null
    group by PFT_ID
    having count(*) > 1

) validation_errors


