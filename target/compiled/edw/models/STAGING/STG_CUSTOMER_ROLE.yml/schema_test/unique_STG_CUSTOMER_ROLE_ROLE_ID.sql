
    
    



select count(*) as validation_errors
from (

    select
        ROLE_ID

    from STAGING.STG_CUSTOMER_ROLE
    where ROLE_ID is not null
    group by ROLE_ID
    having count(*) > 1

) validation_errors


