
    
    



select count(*) as validation_errors
from STAGING.DSV_INJURED_WORKER
where UNIQUE_ID_KEY is null


