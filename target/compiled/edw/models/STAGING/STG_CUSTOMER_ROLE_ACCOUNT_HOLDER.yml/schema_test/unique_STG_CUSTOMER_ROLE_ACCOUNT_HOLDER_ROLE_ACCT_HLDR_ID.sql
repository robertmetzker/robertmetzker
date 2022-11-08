
    
    



select count(*) as validation_errors
from (

    select
        ROLE_ACCT_HLDR_ID

    from STAGING.STG_CUSTOMER_ROLE_ACCOUNT_HOLDER
    where ROLE_ACCT_HLDR_ID is not null
    group by ROLE_ACCT_HLDR_ID
    having count(*) > 1

) validation_errors


