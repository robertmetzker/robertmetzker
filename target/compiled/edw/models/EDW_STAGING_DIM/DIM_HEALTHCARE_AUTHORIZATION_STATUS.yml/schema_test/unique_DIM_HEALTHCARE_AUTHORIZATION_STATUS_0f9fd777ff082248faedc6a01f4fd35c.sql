
    
    



select count(*) as validation_errors
from (

    select
        HEALTHCARE_AUTHORIZATION_STATUS_HKEY

    from EDW_STAGING_DIM.DIM_HEALTHCARE_AUTHORIZATION_STATUS
    where HEALTHCARE_AUTHORIZATION_STATUS_HKEY is not null
    group by HEALTHCARE_AUTHORIZATION_STATUS_HKEY
    having count(*) > 1

) validation_errors


