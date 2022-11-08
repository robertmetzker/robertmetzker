
    
    



select count(*) as validation_errors
from (

    select
        COVERED_INDIVIDUAL_CUSTOMER_HKEY

    from EDW_STAGING_DIM.DIM_COVERED_INDIVIDUAL_CUSTOMER
    where COVERED_INDIVIDUAL_CUSTOMER_HKEY is not null
    group by COVERED_INDIVIDUAL_CUSTOMER_HKEY
    having count(*) > 1

) validation_errors


