
    
    



select count(*) as validation_errors
from STAGING.STG_OPPS_RETURN
where OPPS_RETURN_CODE is null


