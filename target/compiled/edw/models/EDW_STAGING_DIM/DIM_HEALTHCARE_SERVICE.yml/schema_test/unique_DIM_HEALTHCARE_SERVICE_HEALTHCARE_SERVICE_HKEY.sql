
    
    



select count(*) as validation_errors
from (

    select
        HEALTHCARE_SERVICE_HKEY

    from EDW_STAGING_DIM.DIM_HEALTHCARE_SERVICE
    where HEALTHCARE_SERVICE_HKEY is not null
    group by HEALTHCARE_SERVICE_HKEY
    having count(*) > 1

) validation_errors


