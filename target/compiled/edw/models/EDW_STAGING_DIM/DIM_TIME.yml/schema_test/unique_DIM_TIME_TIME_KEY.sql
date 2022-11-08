
    
    



select count(*) as validation_errors
from (

    select
        TIME_KEY

    from EDW_STAGING_DIM.DIM_TIME
    where TIME_KEY is not null
    group by TIME_KEY
    having count(*) > 1

) validation_errors


