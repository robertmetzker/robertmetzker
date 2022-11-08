
    
    



select count(*) as validation_errors
from (

    select
        CASE_STATUS_HKEY

    from EDW_STAGING_DIM.DIM_CASE_STATUS
    where CASE_STATUS_HKEY is not null
    group by CASE_STATUS_HKEY
    having count(*) > 1

) validation_errors


