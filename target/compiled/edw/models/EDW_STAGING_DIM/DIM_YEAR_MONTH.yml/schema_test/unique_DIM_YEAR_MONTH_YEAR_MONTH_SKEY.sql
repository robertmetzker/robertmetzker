
    
    



select count(*) as validation_errors
from (

    select
        YEAR_MONTH_SKEY

    from EDW_STAGING_DIM.DIM_YEAR_MONTH
    where YEAR_MONTH_SKEY is not null
    group by YEAR_MONTH_SKEY
    having count(*) > 1

) validation_errors


