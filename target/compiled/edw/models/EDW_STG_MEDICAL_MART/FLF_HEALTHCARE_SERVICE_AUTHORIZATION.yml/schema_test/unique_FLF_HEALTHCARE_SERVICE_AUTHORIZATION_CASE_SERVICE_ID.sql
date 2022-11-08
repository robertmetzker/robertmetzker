
    
    



select count(*) as validation_errors
from (

    select
        CASE_SERVICE_ID

    from EDW_STG_MEDICAL_MART.FLF_HEALTHCARE_SERVICE_AUTHORIZATION
    where CASE_SERVICE_ID is not null
    group by CASE_SERVICE_ID
    having count(*) > 1

) validation_errors


