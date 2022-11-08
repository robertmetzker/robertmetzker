
    
    



select count(*) as validation_errors
from (

    select
        PRESCRIPTION_HKEY

    from EDW_STAGING_DIM.DIM_PRESCRIPTION_BILL
    where PRESCRIPTION_HKEY is not null
    group by PRESCRIPTION_HKEY
    having count(*) > 1

) validation_errors


