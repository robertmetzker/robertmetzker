
    
    



select count(*) as validation_errors
from (

    select
        CODE

    from STAGING.DST_EOB
    where CODE is not null
    group by CODE
    having count(*) > 1

) validation_errors


