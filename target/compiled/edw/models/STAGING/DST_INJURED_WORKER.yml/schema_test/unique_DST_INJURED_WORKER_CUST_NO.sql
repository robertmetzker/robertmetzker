
    
    



select count(*) as validation_errors
from (

    select
        CUST_NO

    from STAGING.DST_INJURED_WORKER
    where CUST_NO is not null
    group by CUST_NO
    having count(*) > 1

) validation_errors


