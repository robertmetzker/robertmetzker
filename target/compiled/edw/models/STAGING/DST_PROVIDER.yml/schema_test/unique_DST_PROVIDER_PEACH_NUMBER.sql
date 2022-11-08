
    
    



select count(*) as validation_errors
from (

    select
        PEACH_NUMBER

    from STAGING.DST_PROVIDER
    where PEACH_NUMBER is not null
    group by PEACH_NUMBER
    having count(*) > 1

) validation_errors


