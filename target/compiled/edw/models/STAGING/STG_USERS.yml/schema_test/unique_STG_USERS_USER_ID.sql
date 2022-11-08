
    
    



select count(*) as validation_errors
from (

    select
        USER_ID

    from STAGING.STG_USERS
    where USER_ID is not null
    group by USER_ID
    having count(*) > 1

) validation_errors


