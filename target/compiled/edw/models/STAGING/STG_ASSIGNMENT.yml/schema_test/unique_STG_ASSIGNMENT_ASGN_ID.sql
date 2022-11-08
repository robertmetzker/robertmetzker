
    
    



select count(*) as validation_errors
from (

    select
        ASGN_ID

    from STAGING.STG_ASSIGNMENT
    where ASGN_ID is not null
    group by ASGN_ID
    having count(*) > 1

) validation_errors


