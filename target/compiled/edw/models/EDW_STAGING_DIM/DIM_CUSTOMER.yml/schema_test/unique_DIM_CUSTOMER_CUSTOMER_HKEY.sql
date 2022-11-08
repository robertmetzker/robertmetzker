
    
    



select count(*) as validation_errors
from (

    select
        CUSTOMER_HKEY

    from EDW_STAGING_DIM.DIM_CUSTOMER
    where CUSTOMER_HKEY is not null
    group by CUSTOMER_HKEY
    having count(*) > 1

) validation_errors


