
    
    



select count(*) as validation_errors
from (

    select
        TASK_ID

    from STAGING.STG_TASK
    where TASK_ID is not null
    group by TASK_ID
    having count(*) > 1

) validation_errors


