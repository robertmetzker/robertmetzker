
    
    



select count(*) as validation_errors
from (

    select
        CASE_SERV_ID

    from STAGING.DSV_HEALTHCARE_SERVICE_AUTHORIZATION
    where CASE_SERV_ID is not null
    group by CASE_SERV_ID
    having count(*) > 1

) validation_errors


