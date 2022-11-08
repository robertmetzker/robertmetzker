
    
    



select count(*) as validation_errors
from (

    select
        PFTA_ID

    from STAGING.STG_POLICY_FINANCIAL_TRANSACTION_APPLIED
    where PFTA_ID is not null
    group by PFTA_ID
    having count(*) > 1

) validation_errors


