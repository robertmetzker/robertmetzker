
    
    



select count(*) as validation_errors
from STAGING.DSV_HEALTHCARE_SERVICE_AUTHORIZATION
where CASE_SERV_ID is null


