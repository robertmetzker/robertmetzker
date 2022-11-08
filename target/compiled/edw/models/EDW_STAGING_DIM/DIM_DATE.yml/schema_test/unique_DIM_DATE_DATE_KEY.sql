
    
    



select count(*) as validation_errors
from (

    select
        DATE_KEY

    from EDW_STAGING_DIM.DIM_DATE
    where DATE_KEY is not null
    group by DATE_KEY
    having count(*) > 1

) validation_errors


