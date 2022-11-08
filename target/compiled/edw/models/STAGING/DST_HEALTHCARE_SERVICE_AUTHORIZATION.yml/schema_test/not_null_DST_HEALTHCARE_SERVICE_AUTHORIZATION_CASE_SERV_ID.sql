
    
    



select count(*) as validation_errors
from STAGING.DST_HEALTHCARE_SERVICE_AUTHORIZATION
where CASE_SERV_ID is null


